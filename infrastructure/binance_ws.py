import asyncio
import orjson
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
        
        initial_streams = streams or []
        self.streams = set()
        for s in initial_streams:
            if s.startswith("!"):
                self.streams.add(s)
            elif "@" in s:
                symbol, stream_type = s.split("@", 1)
                self.streams.add(f"{symbol.lower()}@{stream_type}")
            else:
                self.streams.add(s.lower())

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

        watchdog_task = asyncio.create_task(self._watchdog())
        system_state.task_registry.register(watchdog_task, "WS_Watchdog")

        if settings.SIMULATION_MODE:
            logger.info("Modo SIMULACIÓN activo para WebSockets.")
            await self._run_simulation()
            return

        while self.is_running:
            try:
                if not self.streams:
                    await asyncio.sleep(2)
                    continue
                    
                stream_url = f"{self.base_url}?streams={'/'.join(self.streams)}"
                logger.info(f"Conectando a Binance WebSocket (Multiplex): {stream_url}")

                async with websockets.connect(
                    stream_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10**7,
                    max_queue=2**6
                ) as ws:
                    self._ws = ws
                    system_state.set_health(HealthStatus.HEALTHY)
                    self._reconnect_delay = 1
                    logger.success("Conexión WebSocket establecida.")

                    try:
                        async for message in ws:
                            if not self.is_running:
                                break
                            self._last_msg_time = time.time()
                            # Procesamiento sincrónico directo para evitar saturación de asyncio
                            self._parse_and_route_sync(message)

                    except websockets.exceptions.ConnectionClosed as e:
                        logger.warning(f"Conexión cerrada por el servidor: {e}")

            except Exception as e:
                system_state.set_health(HealthStatus.DEGRADED)
                if not self.is_running:
                    break

                sleep_time = min(self._reconnect_delay, self._max_reconnect_delay) + random.uniform(0, 0.5)
                logger.warning(f"Reintentando conexión en {sleep_time:.2f}s... Error: {e}")
                await asyncio.sleep(sleep_time)
                self._reconnect_delay *= 1.5

    def _parse_and_route_sync(self, message):
        """
        Parsea el mensaje JSON y lo enruta a la cola de mercado.
        Versión activa con backpressure extremo (purga al 90%).
        """
        try:
            raw_data = orjson.loads(message)
        except Exception:
            self.discarded_count += 1
            return

        # En multiplex, la data viene dentro de 'data' y el stream en 'stream'
        if isinstance(raw_data, dict) and 'stream' in raw_data and 'data' in raw_data:
            data = raw_data['data']
        else:
            data = raw_data

        # --- Métricas de latencia ---
        now = time.time() * 1000  # ms

        event_time = 0
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                event_time = data[0].get('E', 0)
        elif isinstance(data, dict):
            event_time = data.get('E', 0)

        if event_time > 0:
            lag = max(0, now - event_time)
            self.total_lag += lag
            self.msg_count += 1
            if self.msg_count % 500 == 0: # Reducir spam de métricas
                avg_lag = self.total_lag / self.msg_count
                elapsed = time.time() - self.start_time
                tps = self.msg_count / elapsed if elapsed > 0 else 0
                logger.info(
                    f"WS Metrics | Lag: {lag:.2f}ms | Avg: {avg_lag:.2f}ms "
                    f"| TPS: {tps:.2f} | Q: {self.market_queue.qsize()}"
                )

        # --- Backpressure ---
        q_size = self.market_queue.qsize()
        max_q = self.market_queue.maxsize or 1000

        # Nivel crítico (>95%): purga agresiva
        if q_size > max_q * 0.95:
            logger.warning(f"⚠️ PURGA CRÍTICA WS ({q_size}/{max_q}).")
            for _ in range(int(max_q * 0.2)): # Tirar solo 20%
                try:
                    self.market_queue.get_nowait()
                    self.market_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            return

        # Nivel alto (>50%): descartar mensajes no críticos
        elif max_q > 0 and q_size > max_q * 0.5:
            if not self._is_critical_message(data):
                self.discarded_count += 1
                return

        # Encolar
        try:
            self.market_queue.put_nowait(data)
        except asyncio.QueueFull:
            self.discarded_count += 1

    async def subscribe(self, streams: list):
        """Suscribe dinámicamente a nuevos streams en caliente."""
        valid_streams = []
        for s in streams:
            if not s or not isinstance(s, str):
                continue
            if s.startswith("!"):
                valid_streams.append(s)
            elif "@" in s:
                symbol, stream_type = s.split("@", 1)
                valid_streams.append(f"{symbol.lower()}@{stream_type}")
            else:
                valid_streams.append(s.lower())

        new_streams = [s for s in valid_streams if s not in self.streams]

        if not new_streams:
            return

        self.streams.update(new_streams)

        if self._ws and self._ws.state == websockets.State.OPEN:
            try:
                payload = {
                    "method": "SUBSCRIBE",
                    "params": new_streams,
                    "id": int(time.time() * 1000) % 100000
                }
                await self._ws.send(orjson.dumps(payload).decode('utf-8'))
                logger.info(f"WS SUBSCRIBE: {new_streams}")
            except Exception as e:
                logger.error(f"Error enviando SUBSCRIBE: {e}")

    def _is_critical_message(self, data) -> bool:
        """
        Determina si un mensaje es crítico basándose en volatilidad o volumen.
        Prioriza: picos de precio >0.5% en un tick o volumen alto.
        """
        try:
            if isinstance(data, list):
                for tick in data:
                    c = float(tick.get('c', 0))
                    o = float(tick.get('o', 0))
                    if o > 0 and abs(c / o - 1) > 0.005:
                        return True
                    if float(tick.get('v', 0)) > 100:
                        return True
            return False
        except Exception:
            return True  # Ante la duda, es crítico

    async def _watchdog(self):
        """
        Detecta conexiones zombie si no llegan mensajes en >10s.
        """
        logger.info("Watchdog de WebSocket iniciado.")
        while self.is_running:
            await asyncio.sleep(2)

            if self._last_msg_time == 0:
                continue

            elapsed = time.time() - self._last_msg_time

            if elapsed > 10.0:
                logger.error(f"¡CONEXIÓN ZOMBIE DETECTADA! ({elapsed:.2f}s sin datos). Reiniciando...")
                system_state.set_health(HealthStatus.RECOVERING)
                self._last_msg_time = time.time()  # Reset para no spamear logs

    def stop(self):
        self.is_running = False

    async def _run_simulation(self):
        price = 50000.0
        while self.is_running:
            price += random.uniform(-10, 10)
            data = [{"s": "BTCUSDT", "c": str(price), "E": int(time.time() * 1000)}]
            await self.market_queue.put(data)
            await asyncio.sleep(0.5)