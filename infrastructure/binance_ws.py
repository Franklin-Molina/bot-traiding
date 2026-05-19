import asyncio
import json
import websockets
import random
import time
from loguru import logger
from core.config import settings
from core.state import system_state, HealthStatus

class BinanceWS:
    def __init__(self, market_queue: asyncio.Queue, streams: list = None):
        # Usamos /stream para multiplexing
        self.base_url = "wss://stream.binance.com:9443/stream"
        self.market_queue = market_queue
        self.streams = set(streams or ["!miniTicker@arr"])
        self.is_running = False
        self._ws = None
        self._last_msg_time = 0
        self._reconnect_delay = 1
        self._max_reconnect_delay = 3
        
        # Métricas
        self.msg_count = 0
        self.discarded_count = 0
        self.total_lag = 0
        self.start_time = time.time()

    async def connect(self):
        """
        Mantiene la conexión WebSocket activa con reconexión rápida y watchdog.
        """
        self.is_running = True
        
        # Iniciar Watchdog
        watchdog_task = asyncio.create_task(self._watchdog())
        system_state.task_registry.register(watchdog_task, "WS_Watchdog")

        if settings.SIMULATION_MODE:
            logger.info("Modo SIMULACIÓN activo para WebSockets.")
            await self._run_simulation()
            return

        while self.is_running:
            try:
                stream_url = f"{self.base_url}?streams={'/'.join(self.streams)}"
                logger.info(f"Conectando a Binance WebSocket (Multiplex): {stream_url}")
                
                async with websockets.connect(stream_url) as ws:
                    self._ws = ws
                    system_state.set_health(HealthStatus.HEALTHY)
                    self._reconnect_delay = 1 # Reset delay on success
                    logger.success("Conexión WebSocket establecida.")
                    
                    while self.is_running:
                        try:
                            # Timeout para asegurar que el loop no se bloquee si ws.recv() tarda
                            message = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            self._last_msg_time = time.time()
                            await self._process_message(message)
                        except asyncio.TimeoutError:
                            continue # El watchdog se encargará si esto persiste
                        
            except (websockets.exceptions.ConnectionClosed, Exception) as e:
                system_state.set_health(HealthStatus.DEGRADED)
                if not self.is_running:
                    break
                
                # Exponential backoff con jitter
                sleep_time = min(self._reconnect_delay, self._max_reconnect_delay) + random.uniform(0, 0.5)
                logger.warning(f"WebSocket desconectado ({e}). Reintentando en {sleep_time:.2f}s...")
                await asyncio.sleep(sleep_time)
                self._reconnect_delay *= 1.5

    async def subscribe(self, streams: list):
        """Suscribe dinámicamente a nuevos streams."""
        new_streams = [s for s in streams if s not in self.streams]
        if not new_streams: return
        
        self.streams.update(new_streams)
        if self._ws and self._ws.open:
            payload = {
                "method": "SUBSCRIBE",
                "params": new_streams,
                "id": random.randint(1, 10000)
            }
            await self._ws.send(json.dumps(payload))
            logger.info(f"WS SUBSCRIBE: {new_streams}")

    async def unsubscribe(self, streams: list):
        """Desuscribe dinámicamente de streams."""
        to_remove = [s for s in streams if s in self.streams]
        if not to_remove: return
        
        for s in to_remove:
            self.streams.discard(s)
            
        if self._ws and self._ws.open:
            payload = {
                "method": "UNSUBSCRIBE",
                "params": to_remove,
                "id": random.randint(1, 10000)
            }
            await self._ws.send(json.dumps(payload))
            logger.info(f"WS UNSUBSCRIBE: {to_remove}")

    async def _process_message(self, message):
        raw_data = json.loads(message)
        
        # En multiplex, la data viene en 'data' y el stream en 'stream'
        if 'stream' in raw_data and 'data' in raw_data:
            data = raw_data['data']
        else:
            data = raw_data

        now = time.time() * 1000 # ms
        
        # Extraer event time de Binance (E) si existe
        event_time = 0
        if isinstance(data, list) and len(data) > 0:
            event_time = data[0].get('E', 0)
        elif isinstance(data, dict):
            event_time = data.get('E', 0)
            
        if event_time > 0:
            lag = now - event_time
            self.total_lag += lag
            self.msg_count += 1
            if self.msg_count % 100 == 0:
                avg_lag = self.total_lag / self.msg_count
                elapsed = time.time() - self.start_time
                tps = self.msg_count / elapsed if elapsed > 0 else 0
                logger.debug(f"WS Metrics | Lag: {lag:.2f}ms | Avg: {avg_lag:.2f}ms | TPS: {tps:.2f}")

        # 🚀 Adaptive Load Shedding
        # Si la cola está a más del 80%, filtramos mensajes no críticos
        queue_fill_pct = self.market_queue.qsize() / self.market_queue.maxsize if self.market_queue.maxsize > 0 else 0
        
        if queue_fill_pct > 0.8:
            if not self._is_critical_message(data):
                self.discarded_count += 1
                return # Descartar tick tranquilo para dar aire al procesador

        # Backpressure extremo: si está llena, descartar el más antiguo
        if self.market_queue.full():
            self.discarded_count += 1
            try:
                self.market_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        
        await self.market_queue.put(data)

    def _is_critical_message(self, data) -> bool:
        """
        Determina si un mensaje es crítico basándose en volatilidad o volumen.
        Prioriza: Picos de precio (>0.5% en un tick) o volumen masivo.
        """
        try:
            # miniTicker@arr es una lista
            if isinstance(data, list):
                for tick in data:
                    # 'c' current, 'o' open
                    c = float(tick.get('c', 0))
                    o = float(tick.get('o', 0))
                    if o > 0 and abs(c/o - 1) > 0.005: # Movimiento > 0.5%
                        return True
                    if float(tick.get('v', 0)) > 100: # Volumen arbitrario alto para miniTicker
                        return True
            return False
        except:
            return True # Ante la duda, es crítico

    async def _watchdog(self):
        """
        Detecta conexiones zombie basándose en el tráfico esperado.
        """
        logger.info("Watchdog de WebSocket iniciado.")
        while self.is_running:
            await asyncio.sleep(2) # Revisar cada 2 segundos
            
            if self._last_msg_time == 0:
                continue
                
            elapsed = time.time() - self._last_msg_time
            
            # Timeout dinámico: si no hay mensajes en 10s (para miniTicker que es frecuente)
            # En un sistema real, esto se ajustaría por stream.
            if elapsed > 10.0:
                logger.error(f"¡CONEXIÓN ZOMBIE DETECTADA! ({elapsed:.2f}s sin datos). Reiniciando...")
                system_state.set_health(HealthStatus.RECOVERING)
                # Forzar cierre si es necesario (el loop principal de connect reintentará)
                # Aquí podríamos lanzar una excepción o simplemente esperar que el timeout de wait_for falle
                self._last_msg_time = time.time() # Reset para no spamear logs

    def stop(self):
        self.is_running = False

    async def _run_simulation(self):
        price = 50000.0
        while self.is_running:
            price += random.uniform(-10, 10)
            data = [{"s": "BTCUSDT", "c": str(price), "E": int(time.time() * 1000)}]
            await self.market_queue.put(data)
            await asyncio.sleep(0.5)
