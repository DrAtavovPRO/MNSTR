#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DataEngine 4.0 для системы МОНСТР

Модуль для сбора и обработки исторических данных с криптовалютных бирж
с поддержкой мультибиржевой интеграции и оптимизированной обработкой.

Основные особенности:
- Поддержка Binance и MEXC через единый интерфейс
- Эффективное инкрементальное обновление данных
- Оптимизированная обработка с pandas/numpy
- Адаптивное управление параллельной обработкой
- Интеграция стаканов с глубиной 20 уровней
"""

# ===========================================================================
# БЛОК 1: ИМПОРТЫ И КОНФИГУРАЦИЯ
# ===========================================================================
import os
import sys
import json
import math
import time
import shutil
import traceback
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Set, Tuple, Union, Type, Protocol, Callable
from abc import ABC, abstractmethod
import concurrent.futures
import threading
import psutil
from functools import wraps
import requests
from urllib.parse import urlencode

# Управление путями
ROOT_DIR = '/home/monster_trading_v2'
sys.path.insert(0, ROOT_DIR)

# Проверка наличия pandas и numpy
try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Импорт компонентов системы
try:
    from scripts.utils.brain_logger import get_logger, log_execution_time, send_telegram
    from config.brain.config import PATHS, TIMEFRAMES, API, PROCESSING, DATA_ENGINE, DATA_UPDATE
    LOGGER_IMPORTED = True
except ImportError as e:
    LOGGER_IMPORTED = False
    import logging
    import sys
    # В начале файла, рядом с другими импортами
    import logging.handlers

    # После инициализации логгера
    def increase_async_queue_size():
        """Увеличение размера очереди AsyncHandler для предотвращения потерь сообщений"""
        for handler in logger.handlers:
            if isinstance(handler, logging.handlers.QueueHandler):
                # Найдём QueueListener, связанный с этим QueueHandler
                queue = handler.queue
                for listener in logging.handlers.QueueListener._listeners:
                    if getattr(listener, 'queue', None) is queue:
                        # Увеличиваем размер очереди
                        new_queue = queue.__class__(maxsize=50000)  # Увеличиваем до 50000
                        # Копируем все сообщения из старой очереди
                        while not queue.empty():
                            try:
                                new_queue.put(queue.get_nowait())
                            except:
                                break
                        # Устанавливаем новую очередь
                        handler.queue = new_queue
                        listener.queue = new_queue
                        logger.info("Размер очереди AsyncHandler увеличен до 10000")
                        return
        logger.warning("QueueHandler не найден, размер очереди не изменён")

    # Вызываем эту функцию после инициализации логгера
    increase_async_queue_size()
    
    # Настройка базового логгера для сообщения об ошибке
    logging.basicConfig(level=logging.ERROR)
    log_console = logging.getLogger("bootstrap")
    log_console.error(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось импортировать необходимые модули: {e}")
    log_console.error("Проверьте настройки путей и наличие всех зависимостей")
    
    # Базовые функции логирования для диагностики
    def get_logger(name, component='default'):
        logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s'))
        logger.addHandler(handler)
        return logger
    
    def log_execution_time(logger=None, component='default', level=logging.INFO):
        def decorator(func):
            def wrapper(*args, **kwargs):
                import time
                start = time.time()
                try:
                    result = func(*args, **kwargs)
                    print(f"Функция {func.__name__} выполнена за {time.time() - start:.2f} сек")
                    return result
                except Exception as ex:
                    print(f"Ошибка в функции {func.__name__}: {ex}")
                    raise
            return wrapper
        return decorator
    
    def send_telegram(message, priority="normal"):
        print(f"[TELEGRAM {priority}] {message}")
    
    # Завершаем выполнение с кодом ошибки
    sys.exit(1)

# Настройка логгера
logger = get_logger('data_engine', 'data_engine')

# ===========================================================================
# БЛОК 2: ЗАПИСЬ ВРЕМЕНИ ВЫПОЛНЕНИЯ ДЛЯ МОНИТОРИНГА
# ===========================================================================
def record_execution_time(operation_name: str, timeframe: Optional[str], execution_time: float) -> None:
    """
    Запись времени выполнения операций в лог без буферизации (для отладки)
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cpu_percent = min(int(execution_time/os.cpu_count()*100), 99)
        
        # Формируем запись
        if timeframe:
            timing_entry = f"[{timestamp}] {operation_name}: {execution_time:.2f} сек, CPU: {cpu_percent}% (TF: {timeframe})"
        else:
            timing_entry = f"[{timestamp}] {operation_name}: {execution_time:.2f} сек, CPU: {cpu_percent}%"
        
        # Прямая запись в файл, как было раньше
        timing_dir = os.path.join(ROOT_DIR, 'logs', 'timing')
        os.makedirs(timing_dir, exist_ok=True)
        
        timing_file = os.path.join(timing_dir, "time_data_engine.log")
        
        with open(timing_file, 'a') as f:
            f.write(f"{timing_entry}\n")
                
    except Exception as e:
        print(f"Ошибка записи времени выполнения: {e}")

# ===========================================================================
# БЛОК 3: ИНТЕРФЕЙСЫ ОБМЕНА ДАННЫМИ
# ===========================================================================
class ExchangeInterface(ABC):
    """
    Абстрактный базовый класс для источников биржевых данных.
    
    Определяет общий интерфейс, который должны реализовать все классы для работы с биржами.
    Обеспечивает единообразное получение данных OHLCV и стаканов независимо от биржи.
    """
    
    @abstractmethod
    def fetch_klines(self, symbol: str, timeframe: str, start_time: int, limit: int) -> List[List]:
        """
        Получение исторических свечей с биржи
        
        Args:
            symbol: Символ торговой пары
            timeframe: Таймфрейм
            start_time: Время начала в миллисекундах
            limit: Максимальное количество свечей
            
        Returns:
            List[List]: Список свечей в формате биржи
        """
        pass
    
    @abstractmethod
    def fetch_order_book(self, symbol: str, depth: int = 20) -> Dict:
        """
        Получение данных стакана (order book) с биржи
        
        Args:
            symbol: Символ торговой пары
            depth: Глубина стакана
            
        Returns:
            Dict: Данные стакана с покупками и продажами
        """
        pass
    
    @abstractmethod
    def is_pair_available(self, symbol: str) -> bool:
        """
        Проверка доступности торговой пары на бирже
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            bool: True если пара доступна на бирже
        """
        pass
    
    @abstractmethod
    def get_exchange_name(self) -> str:
        """
        Получение названия биржи
        
        Returns:
            str: Название биржи
        """
        pass
    
    @abstractmethod
    def format_symbol(self, base_symbol: str) -> str:
        """
        Форматирование символа в соответствии с требованиями биржи
        
        Args:
            base_symbol: Базовый символ (например, 'BTC')
            
        Returns:
            str: Отформатированный символ для биржи (например, 'BTCUSDT')
        """
        pass
    
    @abstractmethod
    def process_klines(self, klines: List[List], timeframe: str) -> List[Dict]:
        """
        Преобразование данных биржи в унифицированный формат OHLCV
        
        Args:
            klines: Список свечей в формате биржи
            timeframe: Таймфрейм
            
        Returns:
            List[Dict]: Унифицированные данные OHLCV
        """
        pass
    
    @abstractmethod
    def process_order_book(self, order_book: Dict) -> Dict:
        """
        Преобразование данных стакана в унифицированный формат
        
        Args:
            order_book: Данные стакана от биржи
            
        Returns:
            Dict: Унифицированные данные стакана
        """
        pass

# ===========================================================================
# БЛОК 4: РЕАЛИЗАЦИЯ BINANCE
# ===========================================================================
class BinanceExchange(ExchangeInterface):
    """
    Реализация интерфейса обмена данными для Binance
    """
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Инициализация клиента Binance
        
        Args:
            api_key: API ключ (опционально)
            api_secret: API секрет (опционально)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.name = "Binance"
        
        # Настройки из конфигурации
        self.base_url = API['binance']['base_url']
        self.endpoints = API['binance']['endpoints']
        self.timeout = API['binance']['timeout']
        self.retry_count = API['binance']['retry_count']
        self.retry_delay = API['binance']['retry_delay']
        self.default_pair = "USDT"
        
        # Настройки лимитов API
        self.rate_limit_max = API['binance'].get('rate_limit', {}).get('requests_per_minute', 1200)
        self.safety_margin = API['binance'].get('rate_limit', {}).get('safety_margin', 5)
        self.delay_between_requests = API['binance'].get('rate_limit', {}).get('delay_between_requests', 0.05)
        
        # Блокировка для синхронизации запросов
        self.api_lock = threading.RLock()
        self.rate_limit_requests = []
        
        # Инициализация клиента Binance
        self.client = self._init_binance_client()
        self.available_pairs = set()

        # Кэш недоступных пар
        self.unavailable_pairs = set()
        # Кэш последней проверки (Unix timestamp)
        self.last_check_time = {}
        
        # Сессия для REST API
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        # Карта таймфреймов для Binance
        self.timeframe_map = {
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1h',
            '4h': '4h',
            '24h': '1d',
            '7d': '1w'
        }
        
        logger.info(f"Инициализирован клиент Binance")
    
    def _init_binance_client(self):
        """
        Инициализация клиента Binance API
        
        Returns:
            Client: Клиент Binance или None при ошибке
        """
        try:
            # Пытаемся загрузить официальный клиент Binance
            from binance.client import Client
            
            if self.api_key and self.api_secret:
                client = Client(self.api_key, self.api_secret)
                logger.info("Клиент Binance успешно инициализирован с API ключом")
            else:
                client = Client()
                logger.info("Клиент Binance инициализирован без API ключа (ограниченный режим)")
            
            # Кэшируем доступные пары
            self._cache_available_pairs(client)
            
            return client
        except ImportError:
            logger.warning("Библиотека python-binance не установлена. Используем REST API")
            return None
        except Exception as e:
            logger.error(f"Ошибка инициализации клиента Binance: {e}")
            logger.info("Переключение на REST API")
            return None
    
    def _cache_available_pairs(self, client=None):
        """
        Кэширование доступных торговых пар для быстрой проверки
        
        Args:
            client: Клиент Binance API (опционально)
        """
        try:
            if client:
                # Используем клиент Binance, если доступен
                exchange_info = client.get_exchange_info()
                self.available_pairs = {
                    symbol['symbol'] for symbol in exchange_info['symbols']
                    if symbol['status'] == 'TRADING'
                }
            else:
                # Используем REST API
                response = self.session.get(f"{self.base_url}/api/v3/exchangeInfo", timeout=self.timeout)
                if response.status_code == 200:
                    exchange_info = response.json()
                    self.available_pairs = {
                        symbol['symbol'] for symbol in exchange_info['symbols']
                        if symbol['status'] == 'TRADING'
                    }
                else:
                    logger.error(f"Ошибка при запросе exchange_info: {response.status_code}")
                    self.available_pairs = set()
            
            logger.info(f"Кэшировано {len(self.available_pairs)} доступных пар на Binance")
        except Exception as e:
            logger.error(f"Ошибка при кэшировании пар Binance: {e}")
            self.available_pairs = set()

    def set_unavailable_pairs(self, pairs: Set[str], timestamps: Dict[str, float] = None):
        """Установка кэша недоступных пар"""
        self.unavailable_pairs = set(pairs)
        self.last_check_time = timestamps or {}
        logger.info(f"Загружен кэш недоступных пар Binance: {len(self.unavailable_pairs)} записей")

    def get_unavailable_pairs(self) -> Set[str]:
        """Получение множества недоступных пар"""
        return self.unavailable_pairs

    def get_check_timestamps(self) -> Dict[str, float]:
        """Получение времени последней проверки для пар"""
        return self.last_check_time
    
    def _check_rate_limit(self):
        """
        Проверка и управление лимитами API запросов
        """
        with self.api_lock:
            now = time.time()
            
            # Удаляем устаревшие записи (старше 1 минуты)
            self.rate_limit_requests = [t for t in self.rate_limit_requests if now - t < 60]
            
            # Если приближаемся к лимиту, делаем паузу
            if len(self.rate_limit_requests) >= self.rate_limit_max - self.safety_margin:
                oldest_request = self.rate_limit_requests[0]
                wait_time = 60 - (now - oldest_request) + 1  # +1 для запаса
                
                logger.info(f"Приближение к лимиту API Binance. Ожидание {wait_time:.2f} сек")
                time.sleep(max(0, wait_time))
                
                # Обновляем список запросов после ожидания
                now = time.time()
                self.rate_limit_requests = [t for t in self.rate_limit_requests if now - t < 60]
            
            # Добавляем текущий запрос
            self.rate_limit_requests.append(now)
            
            # Небольшая задержка между запросами
            if self.delay_between_requests > 0:
                time.sleep(self.delay_between_requests)
    
    def get_exchange_name(self) -> str:
        return self.name
    
    def format_symbol(self, base_symbol: str) -> str:
        """
        Форматирование символа в формат Binance
        
        Args:
            base_symbol: Базовый символ (например, 'BTC')
            
        Returns:
            str: Символ в формате Binance (например, 'BTCUSDT')
        """
        return f"{base_symbol}{self.default_pair}"
    
    def is_pair_available(self, symbol: str) -> bool:
        """
        Проверка доступности торговой пары на Binance с кэшированием результатов
        """
        formatted_symbol = self.format_symbol(symbol)
        current_time = time.time()
        
        # Проверяем в кэше доступных пар
        if formatted_symbol in self.available_pairs:
            return True
        
        # Проверяем в кэше недоступных пар
        if formatted_symbol in self.unavailable_pairs:
            # Проверяем, не устарел ли кэш (повторная проверка раз в 7 дней)
            last_check = self.last_check_time.get(formatted_symbol, 0)
            cache_age_days = (current_time - last_check) / (24 * 3600)
            
            if cache_age_days < 7:  # Если кэш не старше 7 дней, используем его
                logger.debug(f"Использован кэш недоступных пар для {formatted_symbol} (Binance)")
                return False
            else:
                logger.debug(f"Кэш для {formatted_symbol} устарел ({cache_age_days:.1f} дней), выполняем проверку")
        
        # Если пары нет в кэшах или кэш устарел, проверяем явно
        try:
            self._check_rate_limit()
            
            if self.client:
                # Используем клиент Binance, если доступен
                try:
                    self.client.get_ticker(symbol=formatted_symbol)
                    # Если успешно получили информацию, добавляем в кэш доступных
                    self.available_pairs.add(formatted_symbol)
                    return True
                except Exception as e:
                    error_msg = str(e)
                    # Добавляем в кэш недоступных, если ошибка связана с недоступностью пары
                    if "Invalid symbol" in error_msg or "Unknown symbol" in error_msg:
                        self.unavailable_pairs.add(formatted_symbol)
                        self.last_check_time[formatted_symbol] = current_time
                        logger.debug(f"Пара {formatted_symbol} добавлена в кэш недоступных (Binance)")
                    return False
            else:
                # Используем REST API
                response = self.session.get(
                    f"{self.base_url}/api/v3/ticker/price",
                    params={'symbol': formatted_symbol},
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    self.available_pairs.add(formatted_symbol)
                    return True
                else:
                    # Добавляем в кэш недоступных только если получили определенные коды ошибок
                    if response.status_code in [400, 404]:
                        self.unavailable_pairs.add(formatted_symbol)
                        self.last_check_time[formatted_symbol] = current_time
                        logger.debug(f"Пара {formatted_symbol} добавлена в кэш недоступных (Binance)")
                    return False
        except Exception as e:
            logger.debug(f"Ошибка при проверке пары {formatted_symbol} на Binance: {e}")
            return False
    
    def fetch_klines(self, symbol: str, timeframe: str, start_time: int, limit: int) -> List[List]:
        """
        Получение исторических свечей с Binance
        
        Args:
            symbol: Символ торговой пары (например, 'BTC')
            timeframe: Таймфрейм (например, '1h')
            start_time: Время начала в миллисекундах
            limit: Максимальное количество свечей за запрос
            
        Returns:
            List[List]: Список свечей в формате Binance
        """
        formatted_symbol = self.format_symbol(symbol)
        binance_interval = self.timeframe_map.get(timeframe, '1h')
        
        try:
            # Проверяем лимиты запросов
            self._check_rate_limit()
            
            if self.client:
                # Используем клиент Binance, если доступен
                klines = self.client.get_klines(
                    symbol=formatted_symbol,
                    interval=binance_interval,
                    startTime=start_time,
                    limit=limit
                )
                
                # Обработка "призрачных" монет
                if not klines:
                    logger.debug(f"Binance: Получен пустой ответ для {formatted_symbol} ({timeframe}), добавление в кэш недоступных")
                    current_time = time.time()
                    self.unavailable_pairs.add(formatted_symbol)
                    self.last_check_time[formatted_symbol] = current_time
                    
                return klines
            else:
                # Используем REST API
                params = {
                    'symbol': formatted_symbol,
                    'interval': binance_interval,
                    'startTime': start_time,
                    'limit': limit
                }
                
                response = self.session.get(
                    f"{self.base_url}{self.endpoints['klines']}",
                    params=params,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    klines = response.json()
                    
                    # Обработка "призрачных" монет
                    if not klines:
                        logger.debug(f"Binance: Получен пустой ответ для {formatted_symbol} ({timeframe}), добавление в кэш недоступных")
                        current_time = time.time()
                        self.unavailable_pairs.add(formatted_symbol)
                        self.last_check_time[formatted_symbol] = current_time
                        
                    return klines
                else:
                    logger.error(f"Ошибка при запросе klines: {response.status_code} - {response.text}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка при запросе свечей Binance для {formatted_symbol} ({timeframe}): {e}")
            return []
    
    def fetch_order_book(self, symbol: str, depth: int = 20) -> Dict:
        """
        Получение данных стакана (order book) с Binance
        
        Args:
            symbol: Символ торговой пары (например, 'BTC')
            depth: Глубина стакана (5, 10, 20, 50, 100, 500, 1000)
            
        Returns:
            Dict: Данные стакана с покупками и продажами
        """
        formatted_symbol = self.format_symbol(symbol)
        
        # Ограничиваем глубину стандартными значениями
        if depth not in [5, 10, 20, 50, 100, 500, 1000]:
            depth = min([d for d in [5, 10, 20, 50, 100, 500, 1000] if d >= depth])
        
        try:
            # Проверяем лимиты запросов
            self._check_rate_limit()
            
            if self.client:
                # Используем клиент Binance, если доступен
                order_book = self.client.get_order_book(
                    symbol=formatted_symbol,
                    limit=depth
                )
                return order_book
            else:
                # Используем REST API
                params = {
                    'symbol': formatted_symbol,
                    'limit': depth
                }
                
                response = self.session.get(
                    f"{self.base_url}{self.endpoints['orderbook']}",
                    params=params,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"Ошибка при запросе стакана: {response.status_code} - {response.text}")
                    return {}
        except Exception as e:
            logger.error(f"Ошибка при запросе стакана Binance для {formatted_symbol}: {e}")
            return {}
    
    def process_klines(self, klines: List[List], timeframe: str) -> List[Dict]:
        """
        Преобразование данных Binance в унифицированный формат OHLCV
        
        Args:
            klines: Список свечей в формате Binance
            timeframe: Таймфрейм
            
        Returns:
            List[Dict]: Унифицированные данные OHLCV
        """
        if not klines:
            return []
        
        processed_data = []
        
        for kline in klines:
            try:
                # Преобразуем timestamp из миллисекунд в ISO формат
                open_time = datetime.fromtimestamp(kline[0] / 1000.0, tz=timezone.utc).isoformat()
                close_time = datetime.fromtimestamp(kline[6] / 1000.0, tz=timezone.utc).isoformat()
                
                # Проверяем корректность интервала
                if not self._validate_timeframe_interval(kline[0] / 1000.0, kline[6] / 1000.0, timeframe):
                    continue
                
                # Создаем запись в унифицированном формате OHLCV
                entry = {
                    'time_open': open_time,
                    'time_close': close_time,
                    'quote': {
                        'USD': {
                            'open': float(kline[1]),
                            'high': float(kline[2]),
                            'low': float(kline[3]),
                            'close': float(kline[4]),
                            'volume': float(kline[5]),
                            'market_cap': 0,  # Нет в Binance
                            'timestamp': close_time
                        }
                    }
                }
                
                processed_data.append(entry)
            except (ValueError, IndexError) as e:
                logger.debug(f"Ошибка обработки свечи: {e}")
                continue
        
        return processed_data
    
    def process_order_book(self, order_book: Dict) -> Dict:
        """
        Преобразование данных стакана Binance в унифицированный формат
        
        Args:
            order_book: Данные стакана от Binance API
            
        Returns:
            Dict: Унифицированные данные стакана
        """
        if not order_book or 'bids' not in order_book or 'asks' not in order_book:
            return {}
        
        # Преобразуем timestamp из миллисекунд в ISO формат
        timestamp = datetime.now(timezone.utc).isoformat()
        if 'lastUpdateId' in order_book:
            update_id = order_book['lastUpdateId']
        else:
            update_id = 0
        
        # Преобразуем строковые значения в числовые
        processed_bids = [[float(price), float(qty)] for price, qty in order_book['bids']]
        processed_asks = [[float(price), float(qty)] for price, qty in order_book['asks']]
        
        # Создаем унифицированный формат
        processed_order_book = {
            'timestamp': timestamp,
            'update_id': update_id,
            'bids': processed_bids,
            'asks': processed_asks
        }
        
        return processed_order_book
    
    def _validate_timeframe_interval(self, open_time: float, close_time: float, timeframe: str) -> bool:
        """
        Проверка соответствия интервала указанному таймфрейму
        
        Args:
            open_time: Время открытия в секундах
            close_time: Время закрытия в секундах
            timeframe: Таймфрейм
            
        Returns:
            bool: True если интервал соответствует таймфрейму
        """
        # Ожидаемые интервалы в секундах
        expected_intervals = {
            '5m': 5 * 60,            # 5 минут
            '15m': 15 * 60,          # 15 минут
            '30m': 30 * 60,          # 30 минут
            '1h': 60 * 60,           # 1 час
            '4h': 4 * 60 * 60,       # 4 часа
            '24h': 24 * 60 * 60,     # 1 день
            '7d': 7 * 24 * 60 * 60   # 7 дней
        }
        
        if timeframe not in expected_intervals:
            return True  # Пропускаем проверку для неизвестных таймфреймов
        
        expected_interval = expected_intervals[timeframe]
        actual_interval = close_time - open_time
        
        # Допускаем отклонение в 20%
        allowed_deviation = 0.2
        min_valid = expected_interval * (1 - allowed_deviation)
        max_valid = expected_interval * (1 + allowed_deviation)
        
        return min_valid <= actual_interval <= max_valid
    
# ===========================================================================
# БЛОК 5: РЕАЛИЗАЦИЯ MEXC
# ===========================================================================
class MEXCExchange(ExchangeInterface):
    """
    Реализация интерфейса обмена данными для MEXC
    """
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Инициализация клиента MEXC
        
        Args:
            api_key: API ключ (опционально)
            api_secret: API секрет (опционально)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.name = "MEXC"
        
        # Настройки из конфигурации или значения по умолчанию
        mexc_config = API.get('mexc', {})
        self.base_url = mexc_config.get('base_url', 'https://api.mexc.com')
        self.endpoints = mexc_config.get('endpoints', {
            'klines': '/api/v3/klines',
            'orderbook': '/api/v3/depth',
            'ticker': '/api/v3/ticker/24hr'
        })
        self.timeout = mexc_config.get('timeout', 30)
        self.retry_count = mexc_config.get('retry_count', 2)
        self.retry_delay = mexc_config.get('retry_delay', 2)
        self.default_pair = mexc_config.get('default_pair', 'USDT')
        
        # Настройки лимитов API
        rate_limit_config = mexc_config.get('rate_limit', {})
        if isinstance(rate_limit_config, dict):
            self.rate_limit_max = rate_limit_config.get('requests_per_minute', 5000)
            self.safety_margin = rate_limit_config.get('safety_margin', 5)
            self.delay_between_requests = rate_limit_config.get('delay_between_requests', 0.02)
        else:
            self.rate_limit_max = 5000
            self.safety_margin = 5
            self.delay_between_requests = 0.02
        
        # Блокировка для синхронизации запросов
        self.api_lock = threading.RLock()
        self.rate_limit_requests = []
        
        # Для MEXC используем REST API
        self.session = self._init_session()
        self.available_pairs = set()

        # Кэш недоступных пар
        self.unavailable_pairs = set()
        # Кэш последней проверки (Unix timestamp)
        self.last_check_time = {}
        
        # Карта таймфреймов для MEXC
        self.timeframe_map = {
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '60m',
            '4h': '4h',
            '24h': '1d',
            '7d': '1W'
        }
        
        # Кэшируем доступные пары
        self._cache_available_pairs()
        
        logger.info(f"Инициализирован клиент MEXC")
    
    def _init_session(self):
        """Инициализация HTTP сессии для запросов к API"""
        try:
            session = requests.Session()
            
            # Базовые заголовки
            headers = {'Content-Type': 'application/json'}
            
            # Проверяем настройку public_api_only
            public_api_only = API.get('mexc', {}).get('public_api_only', False)
            
            # Добавляем API ключ только если не ограничены публичным API
            if self.api_key and not public_api_only:
                headers['X-MEXC-APIKEY'] = self.api_key
                logger.info("MEXC: API ключ добавлен в заголовки")
            else:
                logger.info("MEXC: Используется только публичное API без ключа")
            
            session.headers.update(headers)
            return session
        except Exception as e:
            logger.error(f"Ошибка инициализации сессии MEXC: {e}")
            return requests.Session()
        
    def _sign_request(self, params):
        """
        Заглушка для подписи запроса при работе с публичным API
        
        В режиме public_api_only этот метод не выполняет подпись, а просто возвращает 
        параметры без изменений для обеспечения совместимости.
        """
        logger.debug(f"Запрос к публичному API не требует подписи")
        return params
    
    def _cache_available_pairs(self):
        """Кэширование доступных торговых пар для быстрой проверки"""
        if not self.session:
            logger.error("HTTP сессия MEXC не инициализирована")
            return
        
        try:
            # Сначала пробуем более простой эндпоинт defaultSymbols
            response = self.session.get(
                f"{self.base_url}/api/v3/defaultSymbols",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                # Оптимизированный вывод логов без вывода всего списка монет
                if 'data' in data and isinstance(data['data'], list):
                    self.available_pairs = set(data['data'])
                    logger.info(f"Кэшировано {len(self.available_pairs)} пар MEXC через defaultSymbols")
                    # Вывод первых 3 монет для примера
                    if len(data['data']) > 0:
                        sample = ', '.join(data['data'][:3]) + (f" и еще {len(data['data'])-3}" if len(data['data']) > 3 else "")
                        logger.debug(f"Примеры пар MEXC: {sample}")
                    return
            
            # Если не удалось, используем exchangeInfo
            logger.info("Запрос списка пар через exchangeInfo")
            response = self.session.get(
                f"{self.base_url}/api/v3/exchangeInfo",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Расширенная отладка
                logger.debug(f"Образец ответа: {str(data)[:1000]}")
                
                # Обработка различных форматов ответа
                symbols_list = []
                if isinstance(data, dict) and 'symbols' in data:
                    symbols_list = data['symbols']
                elif isinstance(data, list):
                    symbols_list = data
                    
                # Гибкая проверка статуса
                self.available_pairs = set()
                for symbol_data in symbols_list:
                    symbol = symbol_data.get('symbol')
                    status = symbol_data.get('status')
                    
                    if not symbol:
                        continue
                        
                    # Проверка как строки и как числа
                    is_active = False
                    if status in ['ENABLED', 'TRADING', 'online'] or status in [1, '1']:
                        is_active = True
                        
                    if is_active:
                        self.available_pairs.add(symbol)
                
                logger.info(f"Кэшировано {len(self.available_pairs)} пар MEXC через exchangeInfo")
                
                # Если кэш пуст, добавляем базовые пары
                if not self.available_pairs:
                    logger.warning("Кэш пар MEXC пуст! Добавляем базовые пары вручную")
                    basic_pairs = [
                        "BTCUSDT", "ETHUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", 
                        "ADAUSDT", "SOLUSDT", "SHIBUSDT", "DOTUSDT", "LTCUSDT"
                    ]
                    self.available_pairs.update(basic_pairs)
                    logger.info(f"Добавлено {len(basic_pairs)} базовых пар в кэш MEXC")
            else:
                logger.error(f"Ошибка получения пар MEXC: {response.status_code}")
                logger.error(f"Полный ответ: {response.text}")
                
                # Резервное добавление базовых пар
                basic_pairs = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT"]
                self.available_pairs.update(basic_pairs)
                logger.warning(f"Добавлено {len(basic_pairs)} базовых пар в кэш при ошибке")
                
        except Exception as e:
            logger.error(f"Ошибка при кэшировании пар MEXC: {e}")
            logger.debug(f"Трассировка: {traceback.format_exc()}")

    def set_unavailable_pairs(self, pairs: Set[str], timestamps: Dict[str, float] = None):
        """Установка кэша недоступных пар"""
        self.unavailable_pairs = set(pairs)
        self.last_check_time = timestamps or {}
        logger.info(f"Загружен кэш недоступных пар MEXC: {len(self.unavailable_pairs)} записей")

    def get_unavailable_pairs(self) -> Set[str]:
        """Получение множества недоступных пар"""
        return self.unavailable_pairs

    def get_check_timestamps(self) -> Dict[str, float]:
        """Получение времени последней проверки для пар"""
        return self.last_check_time
    
    def _check_rate_limit(self):
        """
        Проверка и управление лимитами API запросов
        """
        with self.api_lock:
            now = time.time()
            
            # Удаляем устаревшие записи (старше 1 минуты)
            self.rate_limit_requests = [t for t in self.rate_limit_requests if now - t < 60]
            
            # Если приближаемся к лимиту, делаем паузу
            if len(self.rate_limit_requests) >= self.rate_limit_max - self.safety_margin:
                oldest_request = self.rate_limit_requests[0]
                wait_time = 60 - (now - oldest_request) + 1  # +1 для запаса
                
                logger.info(f"Приближение к лимиту API MEXC. Ожидание {wait_time:.2f} сек")
                time.sleep(max(0, wait_time))
                
                # Обновляем список запросов после ожидания
                now = time.time()
                self.rate_limit_requests = [t for t in self.rate_limit_requests if now - t < 60]
            
            # Добавляем текущий запрос
            self.rate_limit_requests.append(now)
            
            # Небольшая задержка между запросами
            time.sleep(self.delay_between_requests)
    
    def get_exchange_name(self) -> str:
        return self.name
    
    def format_symbol(self, base_symbol: str) -> str:
        """Форматирование символа в формат MEXC"""
        # Если символ уже содержит USDT, не добавляем суффикс
        if self.default_pair in base_symbol:
            return base_symbol
            
        # Для BTC, ETH и других основных монет можно проверить напрямую
        if base_symbol in ["BTC", "ETH", "XRP", "DOGE", "BNB"]:
            formatted = f"{base_symbol}{self.default_pair}"
            logger.debug(f"Форматирование основного символа: {base_symbol} -> {formatted}")
            return formatted
            
        return f"{base_symbol}{self.default_pair}"
    
    def is_pair_available(self, symbol: str) -> bool:
        """
        Проверка доступности торговой пары на MEXC с кэшированием результатов
        """
        formatted_symbol = self.format_symbol(symbol)
        current_time = time.time()
        
        # Проверяем в кэше доступных пар
        if formatted_symbol in self.available_pairs:
            return True
        
        # Проверяем в кэше недоступных пар
        if formatted_symbol in self.unavailable_pairs:
            # Проверяем, не устарел ли кэш (повторная проверка раз в 7 дней)
            last_check = self.last_check_time.get(formatted_symbol, 0)
            cache_age_days = (current_time - last_check) / (24 * 3600)
            
            if cache_age_days < 7:  # Если кэш не старше 7 дней, используем его
                logger.debug(f"Использован кэш недоступных пар для {formatted_symbol} (MEXC)")
                return False
            else:
                logger.debug(f"Кэш для {formatted_symbol} устарел ({cache_age_days:.1f} дней), выполняем проверку")
        
        # Если пары нет в кэшах или кэш устарел, выполняем явную проверку
        try:
            self._check_rate_limit()
            
            # Выполняем прямой запрос ticker для проверки
            response = self.session.get(
                f"{self.base_url}/api/v3/ticker/price",
                params={'symbol': formatted_symbol},
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                logger.debug(f"Подтверждена доступность пары {formatted_symbol} на MEXC")
                # Добавляем в кэш для будущих проверок
                self.available_pairs.add(formatted_symbol)
                return True
            else:
                # Добавляем в кэш недоступных только если получили определенные коды ошибок
                if response.status_code in [400, 404]:
                    self.unavailable_pairs.add(formatted_symbol)
                    self.last_check_time[formatted_symbol] = current_time
                    logger.debug(f"Пара {formatted_symbol} добавлена в кэш недоступных (MEXC): {response.status_code}")
                return False
        except Exception as e:
            logger.debug(f"Ошибка при проверке пары {formatted_symbol} на MEXC: {e}")
            return False
    
    def fetch_klines(self, symbol: str, timeframe: str, start_time: int, limit: int) -> List[List]:
        formatted_symbol = self.format_symbol(symbol)
        # Используем формат 1W для 7d таймфрейма согласно документации
        mexc_interval = '1W' if timeframe == '7d' else self.timeframe_map.get(timeframe, '60m')
        
        try:
            self._check_rate_limit()
            
            max_limit_per_request = 1000  # Максимальное количество свечей за один запрос MEXC
            all_klines = []
            current_time = start_time
            total_required = limit  # Всего требуется свечей
            
            logger.info(f"Начало запроса данных {formatted_symbol} ({timeframe}) с MEXC, требуется ~{total_required} свечей")
            
            while len(all_klines) < total_required:
                params = {
                    'symbol': formatted_symbol,
                    'interval': mexc_interval,
                    'startTime': current_time,
                    'limit': min(total_required - len(all_klines), max_limit_per_request)
                }
                
                response = self.session.get(
                    f"{self.base_url}{self.endpoints['klines']}",
                    params=params,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    klines = response.json()
                    
                    if not klines:
                        logger.debug(f"Получен пустой ответ от MEXC для {formatted_symbol} ({timeframe})")
                        
                        # Обработка "призрачных" монет
                        if len(all_klines) == 0:  # Если это первый запрос и ответ пустой
                            logger.debug(f"MEXC: Монета {formatted_symbol} возвращает пустые данные, добавление в кэш недоступных")
                            current_time = time.time()
                            self.unavailable_pairs.add(formatted_symbol)
                            self.last_check_time[formatted_symbol] = current_time
                            
                        break
                    
                    all_klines.extend(klines)
                    
                    # Логирование процесса
                    if len(all_klines) % 5000 == 0 or len(all_klines) == total_required:
                        progress = (len(all_klines) / total_required) * 100
                        logger.debug(f"Получено {len(all_klines)}/{total_required} свечей ({progress:.1f}%) для {formatted_symbol} ({timeframe})")
                    
                    # Если получено меньше, чем запрошено, значит достигли конца данных
                    if len(klines) < params['limit']:
                        break
                    
                    # Обновляем время для следующего запроса
                    if klines and len(klines[-1]) > 6:
                        current_time = klines[-1][6] + 1  # Время закрытия последней свечи + 1 мс
                    else:
                        break
                    
                    # Если достигли текущего времени
                    if current_time >= int(time.time() * 1000):
                        break
                    
                    # Пауза для соблюдения лимитов API
                    time.sleep(0.02)
                else:
                    error_msg = f"Ошибка получения свечей MEXC: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    # Если ошибка связана с интервалом, пробуем альтернативные форматы
                    if "Invalid interval" in response.text and mexc_interval == "1W":
                        logger.info(f"Пробуем альтернативный формат интервала для {timeframe}")
                        alternative_intervals = ["1w", "7d", "1M"]
                        for alt_interval in alternative_intervals:
                            params['interval'] = alt_interval
                            logger.info(f"Пробуем интервал {alt_interval} для {formatted_symbol}")
                            alt_response = self.session.get(f"{self.base_url}{self.endpoints['klines']}", 
                                                        params=params, timeout=self.timeout)
                            if alt_response.status_code == 200:
                                mexc_interval = alt_interval  # Запомним рабочий формат
                                klines = alt_response.json()
                                if klines:
                                    all_klines.extend(klines)
                                    logger.info(f"Успешно получены данные с интервалом {alt_interval}")
                                    break
                    break
                    
            logger.info(f"Завершен запрос данных {formatted_symbol} ({timeframe}), получено {len(all_klines)} свечей")
            
            # Если получен пустой ответ при успешном статусе и отсутствии данных, 
            # добавляем монету в кэш недоступных
            if len(all_klines) == 0:
                logger.debug(f"MEXC: Монета {formatted_symbol} возвращает пустые данные, добавление в кэш недоступных")
                current_time = time.time()
                self.unavailable_pairs.add(formatted_symbol)
                self.last_check_time[formatted_symbol] = current_time
                
            return all_klines
            
        except Exception as e:
            logger.error(f"Ошибка при запросе свечей MEXC для {formatted_symbol} ({timeframe}): {e}")
            logger.debug(f"Трассировка: {traceback.format_exc()}")
            return []
    
    def fetch_order_book(self, symbol: str, depth: int = 20) -> Dict:
        """
        Получение данных стакана (order book) с MEXC
        
        Args:
            symbol: Символ торговой пары (например, 'BTC')
            depth: Глубина стакана
            
        Returns:
            Dict: Данные стакана с покупками и продажами
        """
        if not self.session:
            logger.error(f"HTTP сессия MEXC не инициализирована")
            return {}
        
        formatted_symbol = self.format_symbol(symbol)
        
        # Ограничиваем глубину стандартными значениями
        if depth not in [5, 10, 20, 50, 100, 500, 1000]:
            depth = min([d for d in [5, 10, 20, 50, 100, 500, 1000] if d >= depth])
        
        try:
            # Проверяем лимиты запросов
            self._check_rate_limit()
            
            # Параметры запроса
            params = {
                'symbol': formatted_symbol,
                'limit': depth
            }
            
            # Получаем настройку public_api_only из конфигурации
            public_api_only = API.get('mexc', {}).get('public_api_only', True)
            
            # Выполняем запрос как публичный (без подписи)
            headers = {}  # Пустые заголовки для публичного API
            
            # Только если API ключ есть И мы не ограничены публичным API, 
            # добавляем ключ в заголовки
            if self.api_key and not public_api_only:
                headers['X-MEXC-APIKEY'] = self.api_key
                # Здесь должна быть подпись, но мы не используем её для публичного API
            
            response = self.session.get(
                f"{self.base_url}{self.endpoints['orderbook']}",
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Ошибка получения стакана MEXC: {response.status_code} - {response.text}")
                logger.debug(f"Полный ответ: {response.text}")
                return {}
        except Exception as e:
            logger.error(f"Ошибка при запросе стакана MEXC для {formatted_symbol}: {e}")
            return {}
    
    def process_klines(self, klines: List[List], timeframe: str) -> List[Dict]:
        """
        Преобразование данных MEXC в унифицированный формат OHLCV
        
        Примечание: формат MEXC аналогичен формату Binance, поэтому обработка похожа
        
        Args:
            klines: Список свечей в формате, аналогичном Binance
            timeframe: Таймфрейм
            
        Returns:
            List[Dict]: Унифицированные данные OHLCV
        """
        if not klines:
            return []
        
        processed_data = []
        
        for kline in klines:
            try:
                # Преобразуем timestamp из миллисекунд в ISO формат
                open_time = datetime.fromtimestamp(kline[0] / 1000.0, tz=timezone.utc).isoformat()
                close_time = datetime.fromtimestamp(kline[6] / 1000.0, tz=timezone.utc).isoformat()
                
                # Проверяем корректность интервала
                if not self._validate_timeframe_interval(kline[0] / 1000.0, kline[6] / 1000.0, timeframe):
                    continue
                
                # Создаем запись в унифицированном формате OHLCV
                entry = {
                    'time_open': open_time,
                    'time_close': close_time,
                    'quote': {
                        'USD': {
                            'open': float(kline[1]),
                            'high': float(kline[2]),
                            'low': float(kline[3]),
                            'close': float(kline[4]),
                            'volume': float(kline[5]),
                            'market_cap': 0,  # Нет в MEXC
                            'timestamp': close_time
                        }
                    }
                }
                
                processed_data.append(entry)
            except (ValueError, IndexError) as e:
                logger.debug(f"Ошибка обработки свечи MEXC: {e}")
                continue
        
        return processed_data
    
    def process_order_book(self, order_book: Dict) -> Dict:
        """
        Преобразование данных стакана MEXC в унифицированный формат
        
        Args:
            order_book: Данные стакана от MEXC API
            
        Returns:
            Dict: Унифицированные данные стакана
        """
        if not order_book or 'bids' not in order_book or 'asks' not in order_book:
            return {}
        
        # Преобразуем timestamp из миллисекунд в ISO формат
        timestamp = datetime.now(timezone.utc).isoformat()
        if 'lastUpdateId' in order_book:
            update_id = order_book['lastUpdateId']
        else:
            update_id = 0
        
        # Преобразуем строковые значения в числовые
        processed_bids = [[float(price), float(qty)] for price, qty in order_book['bids']]
        processed_asks = [[float(price), float(qty)] for price, qty in order_book['asks']]
        
        # Создаем унифицированный формат
        processed_order_book = {
            'timestamp': timestamp,
            'update_id': update_id,
            'bids': processed_bids,
            'asks': processed_asks
        }
        
        return processed_order_book
    
    def _validate_timeframe_interval(self, open_time: float, close_time: float, timeframe: str) -> bool:
        """
        Проверка соответствия интервала указанному таймфрейму
        
        Args:
            open_time: Время открытия в секундах
            close_time: Время закрытия в секундах
            timeframe: Таймфрейм
            
        Returns:
            bool: True если интервал соответствует таймфрейму
        """
        # Ожидаемые интервалы в секундах
        expected_intervals = {
            '5m': 5 * 60,            # 5 минут
            '15m': 15 * 60,          # 15 минут
            '30m': 30 * 60,          # 30 минут
            '1h': 60 * 60,           # 1 час
            '4h': 4 * 60 * 60,       # 4 часа
            '24h': 24 * 60 * 60,     # 1 день
            '7d': 7 * 24 * 60 * 60   # 7 дней
        }
        
        if timeframe not in expected_intervals:
            return True  # Пропускаем проверку для неизвестных таймфреймов
        
        expected_interval = expected_intervals[timeframe]
        actual_interval = close_time - open_time
        
        # Допускаем отклонение в 20%
        allowed_deviation = 0.2
        min_valid = expected_interval * (1 - allowed_deviation)
        max_valid = expected_interval * (1 + allowed_deviation)
        
        return min_valid <= actual_interval <= max_valid
    
# ===========================================================================
# БЛОК 6: ФАБРИКА ОБМЕНА ДАННЫМИ
# ===========================================================================
class ExchangeFactory:
    """
    Фабрика для создания экземпляров источников биржевых данных.
    
    Позволяет централизованно создавать экземпляры бирж 
    с соответствующими конфигурациями и API ключами.
    """
    
    @staticmethod
    def create_exchange(exchange_name: str) -> Optional[ExchangeInterface]:
        """
        Создание источника данных для указанной биржи
        
        Args:
            exchange_name: Название биржи ('binance', 'mexc')
            
        Returns:
            Optional[ExchangeInterface]: Экземпляр интерфейса обмена данными или None
        """
        # Загрузка переменных окружения для API ключей
        try:
            from dotenv import load_dotenv
            
            env_path = os.path.join(ROOT_DIR, 'config', '.env')
            load_dotenv(env_path)
        except ImportError:
            logger.warning("python-dotenv не установлен, будут использоваться только переменные окружения")
        
        # Создание экземпляра в зависимости от биржи
        if exchange_name.lower() == 'binance':
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_API_SECRET')
            return BinanceExchange(api_key, api_secret)
        
        elif exchange_name.lower() == 'mexc':
            api_key = os.getenv('MEXC_API_KEY')
            api_secret = os.getenv('MEXC_API_SECRET')
            return MEXCExchange(api_key, api_secret)
        
        else:
            logger.error(f"Неизвестная биржа: {exchange_name}")
            return None
    
    @staticmethod
    def create_all_exchanges() -> Dict[str, ExchangeInterface]:
        """
        Создание экземпляров всех поддерживаемых бирж
        
        Returns:
            Dict[str, ExchangeInterface]: Словарь {название_биржи: экземпляр_интерфейса}
        """
        exchanges = {}
        
        for exchange_name in ['binance', 'mexc']:
            exchange = ExchangeFactory.create_exchange(exchange_name)
            if exchange:
                exchanges[exchange_name] = exchange
        
        return exchanges
    
# ===========================================================================
# БЛОК 7: ОБРАБОТЧИК ДАННЫХ
# ===========================================================================
class DataProcessor:
    """
    Класс для обработки, валидации и слияния данных OHLCV.
    
    Обеспечивает эффективную обработку данных с использованием pandas
    и предоставляет методы для преобразования между форматами.
    """
    
    def __init__(self):
        """Инициализация обработчика данных"""
        self.pandas_available = PANDAS_AVAILABLE
        
        if not self.pandas_available:
            logger.warning("pandas не доступен, будет использоваться базовая обработка данных")
    
    def validate(self, data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Валидация данных OHLCV с проверкой соответствия таймфрейму
        
        Args:
            data: Список данных OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Валидированные данные OHLCV
        """
        if not data:
            return []
        
        if self.pandas_available:
            return self._validate_with_pandas(data, timeframe)
        else:
            return self._validate_basic(data, timeframe)
    
    def _validate_with_pandas(self, data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Валидация данных с использованием pandas
        
        Args:
            data: Список данных OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Валидированные данные OHLCV
        """
        try:
            # Создаём DataFrame и затем валидируем
            df = self._data_to_dataframe(data)
            
            if df.empty:
                return []
            
            # Сортировка по времени
            df = df.sort_values('timestamp')
            
            # Удаляем дубликаты
            df = df.drop_duplicates(subset=['timestamp'])
            
            # Проверка и удаление невалидных значений
            # Векторизованный подход без циклов
            df = df[(df['open'] > 0) & (df['high'] > 0) & (df['low'] > 0) & (df['close'] > 0)]
            
            # Проверка соответствия таймфрейму
            df = self._validate_timeframe_consistency(df, timeframe)
            
            # Преобразование обратно в исходный формат
            return self._dataframe_to_data(df)
        except Exception as e:
            logger.error(f"Ошибка при валидации данных с pandas: {e}")
            return self._validate_basic(data, timeframe)
    
    def _validate_basic(self, data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Базовая валидация данных без pandas
        
        Args:
            data: Список данных OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Валидированные данные OHLCV
        """
        valid_data = []
        unique_timestamps = set()
        
        for entry in data:
            # Проверяем наличие необходимых полей
            if 'time_close' not in entry or 'quote' not in entry or 'USD' not in entry['quote']:
                continue
            
            timestamp = entry['time_close']
            
            # Пропускаем дубликаты
            if timestamp in unique_timestamps:
                continue
            
            quote = entry['quote']['USD']
            ohlcv_fields = ['open', 'high', 'low', 'close', 'volume']
            
            # Проверяем наличие всех необходимых полей OHLCV
            if not all(field in quote for field in ohlcv_fields):
                continue
            
            # Проверяем на нулевые значения
            if quote['open'] <= 0 or quote['high'] <= 0 or quote['low'] <= 0 or quote['close'] <= 0:
                continue
            
            # Добавляем запись в результат
            unique_timestamps.add(timestamp)
            valid_data.append(entry)
        
        # Сортируем по времени закрытия
        valid_data.sort(key=lambda x: x['time_close'])
        
        # Проверяем соответствие таймфрейму
        return self._basic_validate_timeframe(valid_data, timeframe)
    
    def _basic_validate_timeframe(self, data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Проверка соответствия таймфрейму без pandas
        
        Args:
            data: Список данных OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Данные с корректными интервалами
        """
        if not data or len(data) < 2:
            return data
        
        # Ожидаемые интервалы в секундах
        expected_intervals = {
            '5m': 5 * 60,
            '15m': 15 * 60,
            '30m': 30 * 60,
            '1h': 60 * 60,
            '4h': 4 * 60 * 60,
            '24h': 24 * 60 * 60,
            '7d': 7 * 24 * 60 * 60
        }
        
        if timeframe not in expected_intervals:
            return data  # Пропускаем проверку для неизвестных таймфреймов
        
        expected_interval = expected_intervals[timeframe]
        allowed_deviation = 0.2  # 20% допустимое отклонение
        min_valid = expected_interval * (1 - allowed_deviation)
        max_valid = expected_interval * (1 + allowed_deviation)
        
        valid_data = [data[0]]  # Первая запись всегда валидна
        
        for i in range(1, len(data)):
            try:
                # Преобразуем строки в datetime
                prev_time = datetime.fromisoformat(data[i-1]['time_close'].replace('Z', '+00:00'))
                curr_time = datetime.fromisoformat(data[i]['time_close'].replace('Z', '+00:00'))
                
                # Вычисляем разницу во времени в секундах
                time_diff = (curr_time - prev_time).total_seconds()
                
                # Проверяем соответствие ожидаемому интервалу
                if min_valid <= time_diff <= max_valid:
                    valid_data.append(data[i])
            except:
                continue  # Пропускаем записи с некорректными временными метками
        
        return valid_data
    
    def merge(self, existing_data: List[Dict], new_data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Объединение существующих и новых данных с валидацией
        
        Args:
            existing_data: Существующие данные OHLCV
            new_data: Новые данные OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Объединенные и валидированные данные OHLCV
        """
        # Если нет существующих данных, возвращаем только новые (валидированные)
        if not existing_data:
            return self.validate(new_data, timeframe) if new_data else []
        
        # Если нет новых данных, возвращаем существующие
        if not new_data:
            return existing_data
        
        if self.pandas_available:
            return self._merge_with_pandas(existing_data, new_data, timeframe)
        else:
            return self._merge_basic(existing_data, new_data, timeframe)
    
    def _merge_with_pandas(self, existing_data: List[Dict], new_data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Оптимизированное объединение данных с использованием pandas
        
        Args:
            existing_data: Существующие данные OHLCV
            new_data: Новые данные OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Объединенные и валидированные данные OHLCV
        """
        try:
            # Преобразуем существующие и новые данные в DataFrames
            df_existing = self._data_to_dataframe(existing_data)
            df_new = self._data_to_dataframe(new_data)
            
            # Если один из DataFrame пуст, возвращаем другой
            if df_existing.empty:
                return self.validate(new_data, timeframe)
            if df_new.empty:
                return existing_data
            
            # Объединяем DataFrame (оптимизировано)
            # Используем concat вместо append для лучшей производительности
            df_merged = pd.concat([df_existing, df_new])
            
            # Удаляем дубликаты, сохраняя более новые данные (оптимизировано)
            # keep='last' сохраняет более новые записи
            df_merged = df_merged.drop_duplicates(subset=['timestamp'], keep='last')
            
            # Сортируем по времени (оптимизировано)
            df_merged = df_merged.sort_values('timestamp')
            
            # Проверяем соответствие таймфрейму (векторизовано)
            df_merged = self._validate_timeframe_consistency(df_merged, timeframe)
            
            # Преобразуем обратно в исходный формат (оптимизировано)
            return self._dataframe_to_data(df_merged)
        except Exception as e:
            logger.error(f"Ошибка при объединении данных с pandas: {e}")
            return self._merge_basic(existing_data, new_data, timeframe)
    
    def _merge_basic(self, existing_data: List[Dict], new_data: List[Dict], timeframe: str) -> List[Dict]:
        """
        Базовое объединение данных без pandas
        
        Args:
            existing_data: Существующие данные OHLCV
            new_data: Новые данные OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            List[Dict]: Объединенные и валидированные данные OHLCV
        """
        # Объединяем существующие и новые данные
        combined = existing_data + new_data
        
        # Используем словарь для удаления дубликатов
        unique_entries = {}
        for entry in combined:
            if 'time_close' in entry:
                # Для каждого уникального timestamp сохраняем последнюю запись
                unique_entries[entry['time_close']] = entry
        
        # Преобразуем в список и сортируем
        merged = list(unique_entries.values())
        merged.sort(key=lambda x: x['time_close'])
        
        # Проверяем соответствие таймфрейму
        return self._basic_validate_timeframe(merged, timeframe)
    
    def _data_to_dataframe(self, data: List[Dict]) -> pd.DataFrame:
        """
        Оптимизированное преобразование данных OHLCV в pandas DataFrame
        
        Args:
            data: Список данных OHLCV
            
        Returns:
            pd.DataFrame: DataFrame с данными OHLCV
        """
        if not data:
            return pd.DataFrame()
        
        # Оптимизированное извлечение данных в формат для DataFrame
        records = []
        
        for item in data:
            if 'time_close' not in item or 'quote' not in item or 'USD' not in item['quote']:
                continue
            
            quote = item['quote']['USD']
            if not all(key in quote for key in ['open', 'high', 'low', 'close', 'volume']):
                continue
            
            records.append({
                'timestamp': item['time_close'],
                'open': quote['open'],
                'high': quote['high'],
                'low': quote['low'],
                'close': quote['close'],
                'volume': quote['volume'],
                'market_cap': quote.get('market_cap', 0)
            })
        
        if not records:
            return pd.DataFrame()
        
        # Создаем DataFrame одним вызовом (оптимизировано)
        df = pd.DataFrame(records)
        
        # Преобразуем timestamp в datetime одним вызовом (векторизовано)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Преобразуем все числовые колонки одним вызовом (векторизовано)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'market_cap']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        
        return df
    
    def _dataframe_to_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Оптимизированное преобразование pandas DataFrame в данные OHLCV
        
        Args:
            df: DataFrame с данными OHLCV
            
        Returns:
            List[Dict]: Список данных OHLCV
        """
        if df.empty:
            return []
        
        # Оптимизированное преобразование обратно в список словарей
        # Используем оптимизированные методы pandas вместо циклов
        
        # Копируем данные один раз для лучшей производительности
        timestamp_list = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z').tolist()
        open_list = df['open'].tolist()
        high_list = df['high'].tolist()
        low_list = df['low'].tolist()
        close_list = df['close'].tolist()
        volume_list = df['volume'].tolist()
        market_cap_list = df['market_cap'].fillna(0).tolist()
        
        # Формируем результат в формате OHLCV
        result = []
        for i in range(len(df)):
            entry = {
                'time_close': timestamp_list[i],
                'quote': {
                    'USD': {
                        'open': float(open_list[i]),
                        'high': float(high_list[i]),
                        'low': float(low_list[i]),
                        'close': float(close_list[i]),
                        'volume': float(volume_list[i]),
                        'market_cap': float(market_cap_list[i]),
                        'timestamp': timestamp_list[i]
                    }
                }
            }
            result.append(entry)
        
        return result
    
    def _validate_timeframe_consistency(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Оптимизированная проверка соответствия данных таймфрейму с pandas
        
        Args:
            df: DataFrame с данными OHLCV
            timeframe: Таймфрейм для проверки
            
        Returns:
            pd.DataFrame: DataFrame с валидными данными
        """
        if df.empty or len(df) < 2:
            return df
        
        # Ожидаемые интервалы для каждого таймфрейма
        expected_intervals = {
            '5m': pd.Timedelta(minutes=5),
            '15m': pd.Timedelta(minutes=15),
            '30m': pd.Timedelta(minutes=30),
            '1h': pd.Timedelta(hours=1),
            '4h': pd.Timedelta(hours=4),
            '24h': pd.Timedelta(days=1),
            '7d': pd.Timedelta(days=7)
        }
        
        if timeframe not in expected_intervals:
            return df  # Пропускаем проверку для неизвестных таймфреймов
        
        # Ожидаемый интервал
        expected_interval = expected_intervals[timeframe]
        
        # Сортируем DataFrame
        df = df.sort_values('timestamp')
        
        # Вычисляем интервалы между соседними точками (векторизовано)
        df['interval'] = df['timestamp'].diff()
        
        # Устанавливаем интервал для первой точки
        if len(df) > 0:
            df.loc[df.index[0], 'interval'] = expected_interval
        
        # Определяем допустимые границы (с отклонением в 20%) (векторизовано)
        allowed_deviation = 0.2
        min_valid = expected_interval * (1 - allowed_deviation)
        max_valid = expected_interval * (1 + allowed_deviation)
        
        # Фильтруем только записи с валидными интервалами (векторизовано)
        df['valid_interval'] = ((df['interval'] >= min_valid) & (df['interval'] <= max_valid))
        
        # Первая запись всегда валидна
        if len(df) > 0:
            df.loc[df.index[0], 'valid_interval'] = True
        
        # Фильтруем DataFrame (векторизовано)
        valid_df = df[df['valid_interval']]
        
        # Удаляем временные колонки (оптимизировано)
        valid_df = valid_df.drop(['interval', 'valid_interval'], axis=1)
        
        # Логируем количество отфильтрованных записей
        filtered_count = len(df) - len(valid_df)
        if filtered_count > 0:
            logger.debug(f"Удалено {filtered_count} записей несоответствующих таймфрейму {timeframe}")
        
        return valid_df
    
# ===========================================================================
# БЛОК 8: МНОГОПОТОЧНЫЙ МЕНЕДЖЕР
# ===========================================================================
class ThreadManager:
    """
    Менеджер для эффективного распределения задач между потоками.
    
    Адаптивно определяет оптимальное количество потоков в зависимости
    от загрузки системы и обеспечивает контроль за выполнением задач.
    """
    
    def __init__(self, max_workers: Optional[int] = None, dynamic_workers: bool = True):
        """
        Инициализация менеджера потоков
        
        Args:
            max_workers: Максимальное количество потоков (None для автоопределения)
            dynamic_workers: Адаптивное определение числа потоков
        """
        self.dynamic_workers = dynamic_workers
        
        # Определяем оптимальное количество потоков
        if max_workers is None:
            if dynamic_workers:
                self.max_workers = self._get_optimal_worker_count()
            else:
                # Используем количество CPU ядер с учетом гипертрединга
                # Умножаем на 2 для IO-bound задач
                self.max_workers = min(32, max(4, os.cpu_count() * 2))
        else:
            self.max_workers = max_workers
        
        logger.info(f"Инициализирован менеджер потоков с {self.max_workers} потоками")
    
    def _get_optimal_worker_count(self) -> int:
        """
        Определение оптимального количества потоков с учетом загрузки системы
        
        Returns:
            int: Оптимальное количество потоков
        """
        try:
            # Получаем информацию о системе
            cpu_count = os.cpu_count() or 4
            
            # Определяем загрузку CPU
            cpu_percent = psutil.cpu_percent(interval=0.5)
            
            # Определяем загрузку памяти
            memory_percent = psutil.virtual_memory().percent
            
            # Вычисляем количество потоков в зависимости от загрузки
            if cpu_percent > 80 or memory_percent > 80:
                # При высокой загрузке ограничиваем количество потоков
                return max(2, cpu_count // 2)
            elif cpu_percent > 60 or memory_percent > 60:
                # При средней загрузке используем количество ядер
                return max(2, cpu_count)
            else:
                # При низкой загрузке можем использовать больше потоков
                # для IO-bound задач, но ограничиваем разумным пределом
                return max(2, min(32, cpu_count * 2))
        except Exception as e:
            logger.debug(f"Ошибка определения оптимального числа потоков: {e}")
            # Возвращаем разумное значение по умолчанию
            return 4
    
    def adjust_worker_count(self) -> int:
        """
        Динамическая корректировка количества потоков
        
        Returns:
            int: Новое количество потоков
        """
        if self.dynamic_workers:
            # Получаем оптимальное количество потоков с учетом текущей загрузки
            new_count = self._get_optimal_worker_count()
            
            # Если изменилось количество потоков, логируем
            if new_count != self.max_workers:
                logger.info(f"Корректировка числа потоков: {self.max_workers} -> {new_count}")
                self.max_workers = new_count
        
        return self.max_workers
    
    def execute_in_parallel(self, tasks: List[Any], task_function: Callable, **kwargs) -> List[Any]:
        """
        Выполнение задач параллельно с адаптивным управлением
        
        Args:
            tasks: Список задач для выполнения
            task_function: Функция для обработки задачи
            **kwargs: Дополнительные параметры для функции
            
        Returns:
            List[Any]: Результаты выполнения задач
        """
        if not tasks:
            return []
        
        # Корректируем количество потоков перед запуском
        if self.dynamic_workers:
            self.adjust_worker_count()
        
        # Ограничиваем количество потоков количеством задач
        actual_workers = min(self.max_workers, len(tasks))
        
        logger.info(f"Запуск {len(tasks)} задач в {actual_workers} потоках")
        
        # Запускаем выполнение в пуле потоков
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Запускаем задачи
            future_to_task = {executor.submit(task_function, task, **kwargs): i for i, task in enumerate(tasks)}
            
            # Обрабатываем результаты по мере их готовности
            for future in concurrent.futures.as_completed(future_to_task):
                task_index = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Логируем прогресс каждые 10% или каждые 10 задач
                    if len(tasks) > 10 and (len(results) % max(1, len(tasks) // 10) == 0 or len(results) % 10 == 0):
                        progress = len(results) / len(tasks) * 100
                        logger.debug(f"Прогресс выполнения: {progress:.1f}% ({len(results)}/{len(tasks)})")
                except Exception as e:
                    logger.error(f"Ошибка при выполнении задачи {task_index}: {e}")
                    # Добавляем None для сохранения консистентности размера результатов
                    results.append(None)
        
        # Сортируем результаты по порядку задач, удаляя None
        return [r for r in results if r is not None]
    
    def distribute_tasks(self, tasks: List[Any], task_function: Callable, batch_size: int = 10, **kwargs) -> List[Any]:
        """
        Распределение задач по пакетам для оптимальной обработки
        
        Args:
            tasks: Список задач для выполнения
            task_function: Функция для обработки задачи
            batch_size: Размер пакета задач
            **kwargs: Дополнительные параметры для функции
            
        Returns:
            List[Any]: Результаты выполнения задач
        """
        if not tasks:
            return []
        
        # Разбиваем задачи на пакеты
        batches = [tasks[i:i + batch_size] for i in range(0, len(tasks), batch_size)]
        
        logger.info(f"Распределение {len(tasks)} задач на {len(batches)} пакетов по {batch_size}")
        
        # Определяем функцию для обработки пакета
        def process_batch(batch):
            return [task_function(task, **kwargs) for task in batch]
        
        # Выполняем обработку пакетов параллельно
        batch_results = self.execute_in_parallel(batches, process_batch)
        
        # Объединяем результаты всех пакетов
        results = []
        for batch_result in batch_results:
            if batch_result:  # Проверяем, что результат пакета не None
                results.extend(batch_result)
        
        return results
    
# ===========================================================================
# БЛОК 9: ОСНОВНОЙ КЛАСС DATA ENGINE
# ===========================================================================
class DataEngine:
    """
    Основной класс для сбора, обработки и хранения исторических данных.
    
    Поддерживает несколько бирж, эффективное инкрементальное обновление,
    оптимизированную обработку данных и мониторинг производительности.
    """
    
    def __init__(self, test_mode: bool = False, test_symbols: Optional[List[str]] = None):
        """
        Инициализация Data Engine
        
        Args:
            test_mode: Режим тестирования на ограниченном наборе монет
            test_symbols: Список символов монет для тестирования
        """
        # Замер времени инициализации
        self.init_start_time = time.time()
        logger.info("Инициализация Data Engine v4.0")
        
        # Общие настройки
        self.start_time = datetime.now()
        self.test_mode = test_mode
        self.test_symbols = test_symbols or ["BTC", "ETH", "XRP"]
        
        # Загрузка настроек из конфигурации
        self.timeframes = TIMEFRAMES
        self.enabled_timeframes = {tf: config.get('enabled', True) 
                                  for tf, config in TIMEFRAMES.items()}
        
        # Настройки обновления данных
        self.incremental_update = DATA_UPDATE.get('incremental', True)
        self.overlap_days = DATA_UPDATE.get('overlap_days', 1)
        self.remove_duplicates = DATA_UPDATE.get('remove_duplicates', True)
        self.fill_gaps = DATA_UPDATE.get('fill_gaps', True)
        
        # Настройки стаканов
        self.orderbooks_enabled = DATA_ENGINE.get('orderbook', {}).get('enabled', True)
        self.orderbook_depth = DATA_ENGINE.get('orderbook', {}).get('depth', 20)
        self.orderbook_update_frequency = DATA_ENGINE.get('orderbook', {}).get('update_frequency', 'daily')

        # Создание необходимых директорий
        self._create_directories()
        
        # Настройки пакетной обработки
        self.batch_size = PROCESSING.get('batch_size', 40)
        self.max_workers = PROCESSING.get('max_workers', 24)
        self.dynamic_worker_adjustment = PROCESSING.get('dynamic_worker_adjustment', True)
        
        # Инициализация компонентов
        self.unavailable_cache = self._load_unavailable_pairs_cache()
        self.thread_manager = ThreadManager(
            max_workers=self.max_workers,
            dynamic_workers=self.dynamic_worker_adjustment
        )
        self.data_processor = DataProcessor()
        
        # Инициализация источников биржевых данных
        self.exchange_priority = DATA_ENGINE.get('exchange', {}).get('preferred', ['binance', 'mexc'])
        if isinstance(self.exchange_priority, str):
            self.exchange_priority = [self.exchange_priority]
        
        self.exchanges = self._init_exchanges_with_cache()
        
        # Создаем статус "инициализация"
        self.create_status_file("initializing")
        
        # Запись времени инициализации
        init_time = time.time() - self.init_start_time
        logger.info(f"Data Engine v4.0 инициализирован за {init_time:.2f} сек")
        record_execution_time("initialization", None, init_time)
    
    def _init_exchanges(self) -> Dict[str, ExchangeInterface]:
        """
        Инициализация источников биржевых данных
        
        Returns:
            Dict[str, ExchangeInterface]: Словарь доступных бирж
        """
        exchanges = {}
        
        # Создаем экземпляры бирж в порядке приоритета
        for exchange_name in self.exchange_priority:
            exchange = ExchangeFactory.create_exchange(exchange_name)
            if exchange:
                exchanges[exchange_name] = exchange
                logger.info(f"Инициализирована биржа: {exchange_name}")
            else:
                logger.warning(f"Не удалось инициализировать биржу: {exchange_name}")
        
        if not exchanges:
            logger.error("Не удалось инициализировать ни одну биржу!")
        
        return exchanges
    
    def _load_unavailable_pairs_cache(self) -> Dict[str, Dict[str, Any]]:
        """
        Загрузка кэша недоступных пар из файла
        
        Returns:
            Dict: Словарь с данными кэша {биржа: {pairs: [...], timestamps: {...}}}
        """
        cache_file = "unavailable_pairs.json"
        cache_dir = PATHS.get('DATA_ENGINE', {}).get('cache', 'data/brain/data_engine/cache/')
        cache_path = os.path.join(ROOT_DIR, cache_dir, cache_file)
        
        try:
            if os.path.exists(cache_path):
                with open(cache_path, 'r') as f:
                    cache_data = json.load(f)
                
                # Преобразуем списки в множества для быстрого поиска
                result = {}
                for exchange, data in cache_data.items():
                    if isinstance(data, dict) and 'pairs' in data:
                        result[exchange] = {
                            'pairs': set(data['pairs']),
                            'timestamps': data.get('timestamps', {})
                        }
                    elif isinstance(data, list):
                        # Обратная совместимость с предыдущим форматом
                        result[exchange] = {
                            'pairs': set(data),
                            'timestamps': {}
                        }
                
                total_pairs = sum(len(data['pairs']) for data in result.values())
                logger.info(f"Загружен кэш недоступных пар: {total_pairs} записей")
                return result
            else:
                logger.info(f"Файл кэша недоступных пар не найден: {cache_path}")
                return {}
        except Exception as e:
            logger.error(f"Ошибка загрузки кэша недоступных пар: {e}")
            return {}

    def _save_unavailable_pairs_cache(self) -> None:
        """Сохранение кэша недоступных пар в файл"""
        cache_file = "unavailable_pairs.json"
        cache_dir = PATHS.get('DATA_ENGINE', {}).get('cache', 'data/brain/data_engine/cache/')
        cache_path = os.path.join(ROOT_DIR, cache_dir, cache_file)
        
        try:
            # Создаем директорию, если её нет
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            
            # Собираем данные кэша со всех бирж
            cache_data = {}
            
            for exchange_name, exchange in self.exchanges.items():
                if hasattr(exchange, 'get_unavailable_pairs'):
                    pairs = exchange.get_unavailable_pairs()
                    timestamps = {}
                    
                    # Получаем метки времени, если доступны
                    if hasattr(exchange, 'get_check_timestamps'):
                        timestamps = exchange.get_check_timestamps()
                    
                    # Сохраняем в словарь для JSON-сериализации
                    cache_data[exchange_name] = {
                        'pairs': list(pairs),
                        'timestamps': timestamps
                    }
            
            # Записываем в файл
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            total_pairs = sum(len(data['pairs']) for data in cache_data.values())
            logger.info(f"Сохранен кэш недоступных пар: {total_pairs} записей")
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша недоступных пар: {e}")

    def _init_exchanges_with_cache(self) -> Dict[str, ExchangeInterface]:
        """
        Инициализация бирж с передачей кэша недоступных пар
        
        Returns:
            Dict[str, ExchangeInterface]: Словарь доступных бирж
        """
        exchanges = {}
        
        # Создаем экземпляры бирж в порядке приоритета
        for exchange_name in self.exchange_priority:
            exchange = ExchangeFactory.create_exchange(exchange_name)
            
            if exchange:
                # Устанавливаем кэш недоступных пар, если биржа поддерживает
                if hasattr(exchange, 'set_unavailable_pairs'):
                    cache_data = self.unavailable_cache.get(exchange_name, {})
                    unavailable_pairs = cache_data.get('pairs', set())
                    timestamps = cache_data.get('timestamps', {})
                    
                    exchange.set_unavailable_pairs(unavailable_pairs, timestamps)
                
                exchanges[exchange_name] = exchange
                logger.info(f"Инициализирована биржа: {exchange_name}")
            else:
                logger.warning(f"Не удалось инициализировать биржу: {exchange_name}")
        
        return exchanges
    
    def _create_directories(self) -> None:
        """Создание необходимых директорий для данных"""
        # Функция для создания директории
        def ensure_dir(path):
            full_path = os.path.join(ROOT_DIR, path)
            os.makedirs(full_path, exist_ok=True)
            return full_path
        
        # Создаем директории из конфигурации
        for component in ['current', 'archive', 'temp', 'cache']:
            path = PATHS.get('DATA_ENGINE', {}).get(component)
            if path:
                ensure_dir(path)
        
        # Создаем директории для таймфреймов и стаканов
        current_dir = PATHS.get('DATA_ENGINE', {}).get('current')
        if current_dir:
            # Создаем директории для каждого таймфрейма
            for timeframe in self.timeframes:
                if self.enabled_timeframes.get(timeframe, True):
                    # Директория для данных OHLCV
                    ensure_dir(os.path.join(current_dir, timeframe))
                    
                    # Директория для стаканов, если они включены
                    if self.orderbooks_enabled:
                        ensure_dir(os.path.join(current_dir, f"{timeframe}_orderbooks"))
    
    def get_historical_data_path(self, symbol: str, timeframe: str) -> str:
        """
        Получение пути к файлу с историческими данными
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            str: Полный путь к файлу данных
        """
        # Получаем путь к текущим данным из конфигурации
        current_dir = PATHS.get('DATA_ENGINE', {}).get('current')
        if not current_dir:
            current_dir = 'data/brain/data_engine/current/'
        
        # Полный путь
        return os.path.join(ROOT_DIR, current_dir, timeframe, f"{symbol}.json")
    
    def get_orderbook_data_path(self, symbol: str, timeframe: str) -> str:
        """
        Получение пути к файлу с данными стакана
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            str: Полный путь к файлу стакана
        """
        # Получаем путь к текущим данным из конфигурации
        current_dir = PATHS.get('DATA_ENGINE', {}).get('current')
        if not current_dir:
            current_dir = 'data/brain/data_engine/current/'
        
        # Полный путь к директории со стаканами
        orderbooks_dir = f"{timeframe}_orderbooks"
        
        # Полный путь к файлу
        return os.path.join(ROOT_DIR, current_dir, orderbooks_dir, f"{symbol}_orderbook.json")
    
    def load_historical_data(self, symbol: str, timeframe: str) -> List[Dict]:
        """
        Загрузка исторических данных из файла
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            List[Dict]: Список данных OHLCV
        """
        filepath = self.get_historical_data_path(symbol, timeframe)
        
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    data = json.load(f)
                
                logger.debug(f"Загружены данные {symbol} ({timeframe}): {len(data)} записей")
                
                # Сразу валидируем загруженные данные
                return self.data_processor.validate(data, timeframe)
            else:
                logger.debug(f"Файл не найден: {filepath}")
                return []
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных {symbol} ({timeframe}): {e}")
            return []
    
    def load_orderbook_data(self, symbol: str, timeframe: str) -> Dict:
        """
        Загрузка данных стакана из файла
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            Dict: Данные стакана или пустой словарь при ошибке
        """
        if not self.orderbooks_enabled:
            return {}
            
        filepath = self.get_orderbook_data_path(symbol, timeframe)
        
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    data = json.load(f)
                
                logger.debug(f"Загружены данные стакана {symbol} ({timeframe})")
                return data
            else:
                logger.debug(f"Файл стакана не найден: {filepath}")
                return {}
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных стакана {symbol} ({timeframe}): {e}")
            return {}
    
    def save_historical_data(self, symbol: str, timeframe: str, data: List[Dict]) -> bool:
        """
        Безопасное сохранение исторических данных в файл
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            data: Данные для сохранения
            
        Returns:
            bool: True если сохранение успешно
        """
        # Проверка наличия данных
        if not data:
            logger.warning(f"Пустые данные для сохранения: {symbol} ({timeframe})")
            return False
        
        # Валидируем данные перед сохранением
        valid_data = self.data_processor.validate(data, timeframe)
        
        if not valid_data:
            logger.warning(f"После валидации не осталось данных для сохранения: {symbol} ({timeframe})")
            return False
        
        # Путь к файлу
        filepath = self.get_historical_data_path(symbol, timeframe)
        
        # Путь для временного файла
        temp_dir = PATHS.get('DATA_ENGINE', {}).get('temp')
        if not temp_dir:
            temp_dir = 'data/brain/data_engine/temp/'
        
        temp_path = os.path.join(ROOT_DIR, temp_dir, f"{symbol}_{timeframe}_{int(time.time())}.json")
        
        try:
            # Создаем директории, если их нет
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            
            # Сохраняем во временный файл
            with open(temp_path, 'w') as f:
                json.dump(valid_data, f, indent=2)
            
            # Перемещаем временный файл на место постоянного
            shutil.move(temp_path, filepath)
            
            logger.debug(f"Сохранено данных {symbol} ({timeframe}): {len(valid_data)} записей")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных {symbol} ({timeframe}): {e}")
            
            # Удаляем временный файл при ошибке
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            
            return False
    
    def save_orderbook_data(self, symbol: str, timeframe: str, orderbook_data: Dict) -> bool:
        """
        Сохранение данных стакана в файл
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            orderbook_data: Данные стакана
            
        Returns:
            bool: True если сохранение успешно
        """
        if not self.orderbooks_enabled or not orderbook_data:
            return False
        
        # Путь к файлу
        filepath = self.get_orderbook_data_path(symbol, timeframe)
        
        # Путь для временного файла
        temp_dir = PATHS.get('DATA_ENGINE', {}).get('temp')
        if not temp_dir:
            temp_dir = 'data/brain/data_engine/temp/'
        
        temp_path = os.path.join(ROOT_DIR, temp_dir, f"{symbol}_{timeframe}_orderbook_{int(time.time())}.json")
        
        try:
            # Создаем директории, если их нет
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            
            # Сохраняем во временный файл
            with open(temp_path, 'w') as f:
                json.dump(orderbook_data, f, indent=2)
            
            # Перемещаем временный файл на место постоянного
            shutil.move(temp_path, filepath)
            
            logger.debug(f"Сохранены данные стакана {symbol} ({timeframe})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных стакана {symbol} ({timeframe}): {e}")
            
            # Удаляем временный файл при ошибке
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            
            return False
    
    def normalize_timestamp(self, timestamp_dt: Optional[datetime]) -> Optional[datetime]:
        """
        Нормализация временной метки для корректного сравнения
        
        Args:
            timestamp_dt: Временная метка
            
        Returns:
            Optional[datetime]: Нормализованная временная метка или None
        """
        if not timestamp_dt:
            return None
        
        # Гарантируем UTC для корректного сравнения
        if timestamp_dt.tzinfo is None:
            timestamp_dt = timestamp_dt.replace(tzinfo=timezone.utc)
        
        return timestamp_dt
    
    def get_last_data_date(self, data: List[Dict]) -> Optional[datetime]:
        """
        Определение последней даты в данных с нормализацией
        
        Args:
            data: Список данных OHLCV
            
        Returns:
            Optional[datetime]: Последняя дата или None
        """
        if not data:
            return None
        
        try:
            # Сортируем данные по времени закрытия (по убыванию)
            sorted_data = sorted(data, key=lambda x: x.get('time_close', ''), reverse=True)
            
            if sorted_data and 'time_close' in sorted_data[0]:
                # Получаем последнюю временную метку
                time_str = sorted_data[0]['time_close']
                
                # Конвертируем в datetime с обработкой различных форматов
                if 'Z' in time_str:
                    # Z означает UTC
                    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                elif '+' in time_str or '-' in time_str and 'T' in time_str:
                    # Здесь добавляем обработку формата без двоеточия в часовом поясе
                    if '+' in time_str and ':' not in time_str.split('+')[1]:
                        offset = time_str.split('+')[1]
                        if len(offset) == 4:  # формат +0000
                            reformatted = time_str.split('+')[0] + '+' + offset[:2] + ':' + offset[2:]
                            dt = datetime.fromisoformat(reformatted)
                        else:
                            dt = datetime.fromisoformat(time_str)
                    else:
                        dt = datetime.fromisoformat(time_str)
                else:
                    # Без информации о часовом поясе, предполагаем UTC
                    dt = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
                
                # Нормализуем временную метку
                return self.normalize_timestamp(dt)
            
            return None
        except Exception as e:
            logger.error(f"Ошибка при определении последней даты: {e}")
            return None
    
    def get_historical_depth(self, timeframe: str, existing_data: Optional[List[Dict]] = None) -> int:
        """
        Определяет глубину исторических данных для загрузки
        """
        tf_config = self.timeframes.get(timeframe, {})
        default_days = tf_config.get('history_days', 30)
        
        # Добавить эти строки в начале метода
        update_thresholds = {
            '5m': 0.5,   # Обновлять если данные старше 30 минут
            '15m': 0.5,  # Обновлять если данные старше 30 минут
            '30m': 1,    # Обновлять если данные старше 1 часа
            '1h': 2,     # Обновлять если данные старше 2 часов
            '4h': 4,     # Обновлять если данные старше 4 часов
            '24h': 6,    # Обновлять если данные старше 6 часов
            '7d': 24     # Обновлять если данные старше 24 часов
        }
        
        if self.incremental_update and existing_data:
            last_date = self.get_last_data_date(existing_data)
            
            if last_date:
                now = datetime.now(timezone.utc)
                last_date = self.normalize_timestamp(last_date)
                days_diff = (now - last_date).total_seconds() / (24 * 3600)
                
                if days_diff < 1:
                    hours_diff = days_diff * 24
                    
                    # ЗДЕСЬ заменить существующее условие
                    update_threshold = update_thresholds.get(timeframe, 2)  # По умолчанию 2 часа
                    if hours_diff < update_threshold:
                        logger.info(f"Данные {timeframe} обновлялись недавно ({hours_diff:.1f} ч назад), пропускаем")
                        return 0
                
                days_to_fetch = days_diff + (self.overlap_days / 2)
                
                # ЗДЕСЬ заменить существующие условия для max_days
                if timeframe == '5m':
                    max_days = tf_config.get('history_days', 5)  # 5 дней для 5-минутного таймфрейма
                elif timeframe == '15m':
                    max_days = tf_config.get('history_days', 15)  # 15 дней для 15-минутного таймфрейма
                elif timeframe == '30m':
                    max_days = tf_config.get('history_days', 30)  # 30 дней для 30-минутного таймфрейма
                elif timeframe == '1h':
                    max_days = tf_config.get('history_days', 90)  # 90 дней для 1-часового таймфрейма
                elif timeframe == '4h':
                    max_days = tf_config.get('history_days', 120)  # 120 дней для 4-часового таймфрейма
                elif timeframe == '24h':
                    max_days = tf_config.get('history_days', 150)  # 150 дней для дневного таймфрейма
                elif timeframe == '7d':
                    max_days = tf_config.get('history_days', 365)  # 365 дней для недельного таймфрейма
                else:
                    max_days = default_days
                    
                return min(days_to_fetch, max_days)
        
        return default_days
    
    def should_update_timeframe(self, timeframe: str, force_update: bool = False) -> bool:
        """
        Проверяет, нужно ли обновлять данные для таймфрейма
        
        Args:
            timeframe: Таймфрейм
            force_update: Если True, всегда возвращает True
            
        Returns:
            bool: True если нужно обновить данные
        """
        if force_update:
            return True
            
        if not self.enabled_timeframes.get(timeframe, False):
            return False
        
        update_freq = self.timeframes.get(timeframe, {}).get('update_frequency', 'daily')
        
        current_day = datetime.now().weekday()
        
        if self.test_mode:
            return True
        
        if update_freq == 'weekly':
            return current_day == 0  # Понедельник
        
        return True
    
    def should_update_orderbook(self, timeframe: str) -> bool:
        """
        Проверка необходимости обновления стакана
        
        Args:
            timeframe: Таймфрейм для проверки
            
        Returns:
            bool: True если стакан нужно обновить
        """
        # Проверяем, включены ли стаканы
        if not self.orderbooks_enabled:
            return False
        
        # В тестовом режиме всегда обновляем
        if self.test_mode:
            return True
        
        # Используем ту же логику, что и для исторических данных
        return self.should_update_timeframe(timeframe)
    
    def fetch_data_for_symbol(self, symbol: str, timeframe: str, days_back: int) -> List[Dict]:
        """
        Получение данных OHLCV с бирж для указанного символа
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            days_back: Количество дней для запроса
            
        Returns:
            List[Dict]: Список данных OHLCV
        """
        # Если не нужно обновлять данные
        if days_back <= 0:
            return []
        
        # Начальное время для логирования
        start_time = time.time()
        
        # Счетчик попыток для бирж
        attempt_count = 0
        
        # Пробуем получить данные с бирж в порядке приоритета
        for exchange_name in self.exchange_priority:
            if exchange_name not in self.exchanges:
                continue
            
            attempt_count += 1
            exchange = self.exchanges[exchange_name]
            
            # Проверяем доступность пары на бирже
            if not exchange.is_pair_available(symbol):
                logger.debug(f"Пара {symbol} недоступна на бирже {exchange_name}")
                continue
            
            try:
                # Вычисляем начальное время для запроса
                start_timestamp = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)
                
                # Максимальное количество свечей за запрос
                limit = 1000  # Стандартное ограничение для большинства бирж
                
                # Рассчитываем ожидаемое количество свечей для определения необходимости пагинации
                candles_per_day = {
                    '5m': 288,   # 288 свечей 5м в сутки
                    '15m': 96,   # 96 свечей 15м в сутки
                    '30m': 48,   # 48 свечей 30м в сутки
                    '1h': 24,    # 24 свечи 1ч в сутки
                    '4h': 6,     # 6 свечей 4ч в сутки
                    '24h': 1,    # 1 свеча в сутки
                    '7d': 1/7    # ~0.14 свечи в сутки
                }.get(timeframe, 24)
                
                # Ожидаемое количество свечей
                expected_candles = max(1, int(candles_per_day * days_back))
                
                # Лог запроса
                logger.info(f"Запрос данных {symbol} ({timeframe}) за {days_back} дней c {exchange_name}, ожидается ~{expected_candles} свечей")
                
                # Получаем данные от биржи
                all_klines = []
                
                # Если требуется пагинация
                if expected_candles > limit:
                    current_time = start_timestamp
                    
                    # Получаем данные по частям
                    while True:
                        # Запрашиваем порцию данных
                        klines = exchange.fetch_klines(symbol, timeframe, current_time, limit)
                        
                        # Если порция пустая, прекращаем
                        if not klines:
                            break
                            
                        all_klines.extend(klines)
                        
                        # Если получено меньше limit свечей, достигнут конец периода
                        if len(klines) < limit:
                            break
                            
                        # Время последней свечи + 1 мс для следующего запроса
                        current_time = klines[-1][6] + 1
                        
                        # Если достигли текущего времени
                        if current_time >= int(datetime.now(timezone.utc).timestamp() * 1000):
                            break
                        
                        # Пауза для соблюдения ограничений API
                        time.sleep(0.5)
                else:
                    # Для небольшого количества свечей запрашиваем все сразу
                    all_klines = exchange.fetch_klines(symbol, timeframe, start_timestamp, expected_candles)
                
                # Обрабатываем полученные данные
                if all_klines:
                    ohlcv_data = exchange.process_klines(all_klines, timeframe)
                    
                    # Логируем результат
                    execution_time = time.time() - start_time
                    record_execution_time(f"fetch_{exchange_name}_{symbol}", timeframe, execution_time)
                    
                    logger.info(f"Получено {len(ohlcv_data)} записей для {symbol} ({timeframe}) с {exchange_name} за {execution_time:.2f} сек")
                    
                    return ohlcv_data
                else:
                    logger.warning(f"Нет данных для {symbol} ({timeframe}) на {exchange_name}")
            except Exception as e:
                logger.error(f"Ошибка при запросе данных {symbol} ({timeframe}) с {exchange_name}: {e}")
        
        # Если все биржи не смогли предоставить данные
        if attempt_count > 0:
            logger.warning(f"Не удалось получить данные для {symbol} ({timeframe}) ни с одной из {attempt_count} бирж")
        else:
            logger.warning(f"Нет доступных бирж для получения данных {symbol} ({timeframe})")
        
        # Запись времени выполнения неудачного запроса
        execution_time = time.time() - start_time
        record_execution_time(f"fetch_failed_{symbol}", timeframe, execution_time)
        
        return []
    
    def fetch_orderbook_for_symbol(self, symbol: str, depth: int = 20) -> Dict:
        """
        Получение данных стакана с бирж для указанного символа
        
        Args:
            symbol: Символ монеты
            depth: Глубина стакана
            
        Returns:
            Dict: Данные стакана
        """
        if not self.orderbooks_enabled:
            return {}
            
        # Начальное время для логирования
        start_time = time.time()
        
        # Пробуем получить данные с бирж в порядке приоритета
        for exchange_name in self.exchange_priority:
            if exchange_name not in self.exchanges:
                continue
            
            exchange = self.exchanges[exchange_name]
            
            # Проверяем доступность пары на бирже
            if not exchange.is_pair_available(symbol):
                logger.debug(f"Пара {symbol} недоступна на бирже {exchange_name}")
                continue
            
            try:
                # Запрашиваем стакан
                orderbook_data = exchange.fetch_order_book(symbol, depth)
                
                if orderbook_data and 'bids' in orderbook_data and 'asks' in orderbook_data:
                    # Обрабатываем полученные данные
                    processed_orderbook = exchange.process_order_book(orderbook_data)
                    
                    # Логируем результат
                    execution_time = time.time() - start_time
                    logger.info(f"Получены данные стакана для {symbol} с {exchange_name} за {execution_time:.2f} сек")
                    
                    return processed_orderbook
                else:
                    logger.warning(f"Нет данных стакана для {symbol} на {exchange_name}")
            except Exception as e:
                logger.error(f"Ошибка при запросе стакана {symbol} с {exchange_name}: {e}")
        
        # Если все биржи не смогли предоставить данные
        logger.warning(f"Не удалось получить данные стакана для {symbol} ни с одной биржи")
        
        return {}
    
    def update_timeframe_data(self, symbol: str, timeframe: str) -> Dict:
        """
        Обновление данных для указанного таймфрейма с инкрементальным подходом
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            Dict: Результат обновления
        """
        start_time = time.time()
        
        try:
            # Загружаем существующие данные
            existing_data = self.load_historical_data(symbol, timeframe)
            
            # Определяем глубину истории с учетом инкрементального обновления и таймфрейма
            days_back = self.get_historical_depth(timeframe, existing_data)
            
            # Если данные не требуют обновления
            if days_back <= 0:
                return {
                    "success": True,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "skipped",
                    "message": "Данные актуальны, обновление не требуется",
                    "existing_count": len(existing_data)
                }
            
            # Логируем тип обновления
            if existing_data:
                logger.info(f"Обновление {symbol} ({timeframe}): +{days_back:.2f} дней с последнего обновления")
            else:
                logger.info(f"Первичная загрузка {symbol} ({timeframe}): {days_back} дней")
            
            # Получаем новые данные с бирж
            new_data = self.fetch_data_for_symbol(symbol, timeframe, days_back)
            
            # Получаем данные стакана, если включено
            orderbook_result = False
            if self.should_update_orderbook(timeframe):
                orderbook_data = self.fetch_orderbook_for_symbol(symbol, self.orderbook_depth)
                if orderbook_data:
                    orderbook_result = self.save_orderbook_data(symbol, timeframe, orderbook_data)
            
            if new_data:
                # Объединяем с существующими данными с учетом таймфрейма
                merged_data = self.data_processor.merge(existing_data, new_data, timeframe)
                
                # Сохраняем результат
                success = self.save_historical_data(symbol, timeframe, merged_data)
                
                # Записываем время выполнения
                execution_time = time.time() - start_time
                record_execution_time(f"update_{symbol}", timeframe, execution_time)
                
                return {
                    "success": success,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "updated",
                    "existing_count": len(existing_data),
                    "new_count": len(new_data),
                    "merged_count": len(merged_data),
                    "orderbook_updated": orderbook_result,
                    "execution_time": execution_time
                }
            else:
                # Если не удалось получить новые данные
                execution_time = time.time() - start_time
                
                # Если есть существующие данные, возвращаем частичный успех
                if existing_data:
                    logger.warning(f"Не удалось получить новые данные для {symbol} ({timeframe}), используем существующие")
                    return {
                        "success": True,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "status": "existing_data_kept",
                        "message": "Не удалось получить новые данные, используются существующие",
                        "existing_count": len(existing_data),
                        "orderbook_updated": orderbook_result,
                        "execution_time": execution_time
                    }
                else:
                    logger.warning(f"Не удалось получить данные для {symbol} ({timeframe})")
                    return {
                        "success": False,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "status": "failed",
                        "message": "Не удалось получить данные от бирж",
                        "orderbook_updated": orderbook_result,
                        "execution_time": execution_time
                    }
        except Exception as e:
            # Записываем время выполнения при ошибке
            execution_time = time.time() - start_time
            record_execution_time(f"update_error_{symbol}", timeframe, execution_time)
            
            logger.error(f"Ошибка обновления {symbol} ({timeframe}): {e}")
            
            return {
                "success": False,
                "symbol": symbol,
                "timeframe": timeframe,
                "status": "error",
                "message": str(e),
                "execution_time": execution_time
            }
    
    def process_symbol(self, symbol: str) -> Dict:
        start_time = time.time()
        logger.info(f"Обработка монеты: {symbol}")
        
        timeframe_results = {}
        successful_tfs = 0
        total_tfs = 0
        is_coin_available = True  # Новый флаг для отслеживания доступности монеты
        
        try:
            # Сначала обрабатываем основные таймфреймы (от большего к меньшему)
            # Это важно, так как данные крупных таймфреймов могут использоваться для проверки малых
            priority_timeframes = ['7d', '24h', '4h', '1h', '30m', '15m', '5m']
            
            for timeframe in priority_timeframes:
                if timeframe in self.timeframes and self.should_update_timeframe(timeframe):
                    total_tfs += 1
                    
                    # Пропускаем обработку, если монета уже помечена как недоступная
                    if not is_coin_available:
                        # Создаем запись о пропуске таймфрейма
                        timeframe_results[timeframe] = {
                            "success": False,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "status": "skipped",
                            "message": "Монета недоступна на биржах (определено на предыдущем таймфрейме)"
                        }
                        continue
                    
                    # Пытаемся обновить данные для текущего таймфрейма
                    result = self.update_timeframe_data(symbol, timeframe)
                    timeframe_results[timeframe] = result
                    
                    if result.get('success', False):
                        successful_tfs += 1
                    else:
                        # Проверяем, является ли ошибка связанной с недоступностью монеты
                        status = result.get('status', '')
                        if status == 'failed' and 'Не удалось получить данные от бирж' in result.get('message', ''):
                            # Монета недоступна, прекращаем обработку следующих таймфреймов
                            is_coin_available = False
                            logger.info(f"Монета {symbol} недоступна на таймфрейме {timeframe}, последующие таймфреймы будут пропущены")
            
            # Общее время обработки
            execution_time = time.time() - start_time
            record_execution_time(f"process_symbol_{symbol}", None, execution_time)
            
            # Итоговый результат
            status = "success" if successful_tfs == total_tfs else "partial_success" if successful_tfs > 0 else "failed"
            logger.info(f"Монета {symbol} обработана ({status}) за {execution_time:.2f} сек: {successful_tfs}/{total_tfs} таймфреймов")
            
            return {
                "symbol": symbol,
                "success": successful_tfs > 0,
                "status": status,
                "timeframes_total": total_tfs,
                "timeframes_success": successful_tfs,
                "timeframes_results": timeframe_results,
                "execution_time": execution_time
            }
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Ошибка при обработке монеты {symbol}: {e}")
            
            return {
                "symbol": symbol,
                "success": False,
                "status": "error",
                "message": str(e),
                "execution_time": execution_time
            }
    
    def get_filtered_symbols(self) -> List[str]:
        """
        Получение отфильтрованных символов монет из результатов fetch_market_data.py
        
        Returns:
            List[str]: Список символов монет
        """
        try:
            # Проверяем несколько возможных путей к файлу
            possible_paths = [
                # Основной путь через конфигурацию
                os.path.join(ROOT_DIR, PATHS.get('MARKET_CURRENT', 'data/market/current/'), 'filtered_data.json'),
                # Прямой путь (наиболее надежный вариант)
                '/home/monster_trading_v2/data/market/current/filtered_data.json',
                # Путь относительно текущей директории
                os.path.join(os.getcwd(), 'data/market/current/filtered_data.json')
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    logger.info(f"Найден файл с отфильтрованными монетами: {path}")
                    with open(path, 'r') as f:
                        filtered_data = json.load(f)
                    break
            else:
                logger.error(f"Файл с отфильтрованными монетами не найден. Проверены пути: {possible_paths}")
                return []
            
            # Извлекаем символы
            symbols = [coin.get('symbol') for coin in filtered_data if 'symbol' in coin]
            
            logger.info(f"Загружено {len(symbols)} символов монет из {path}")
            
            # В тестовом режиме ограничиваем количество монет
            if self.test_mode:
                # Находим пересечение тестовых символов с загруженными
                test_symbols = [s for s in self.test_symbols if s in symbols]
                
                # Если не все тестовые символы найдены, добавляем остальные из начала списка
                if len(test_symbols) < len(self.test_symbols):
                    additional_symbols = [s for s in symbols if s not in test_symbols]
                    test_symbols.extend(additional_symbols[:len(self.test_symbols) - len(test_symbols)])
                
                logger.info(f"Тестовый режим: используем {len(test_symbols)} монет: {test_symbols}")
                return test_symbols[:len(self.test_symbols)]
            
            return symbols
        except Exception as e:
            logger.error(f"Ошибка при получении списка символов: {e}")
            logger.debug(f"Стек вызовов: {traceback.format_exc()}")
            
            # В случае ошибки, если тестовый режим, возвращаем тестовые символы
            if self.test_mode:
                logger.warning(f"Возвращаем тестовые символы по умолчанию: {self.test_symbols}")
                return self.test_symbols
            return []
    
    def archive_monthly(self) -> bool:
        """
        Ежемесячное архивирование данных
        
        Returns:
            bool: True если архивирование выполнено успешно
        """
        try:
            # Проверяем, нужно ли архивирование
            if not self._monthly_archive_needed():
                logger.info("Ежемесячное архивирование не требуется")
                return False
            
            logger.info("Начало ежемесячного архивирования данных")
            
            # Получаем пути
            current_dir = PATHS.get('DATA_ENGINE', {}).get('current')
            archive_dir = PATHS.get('DATA_ENGINE', {}).get('archive')
            
            if not current_dir or not archive_dir:
                logger.error("Не определены пути для архивирования")
                return False
            
            # Полные пути
            current_path = os.path.join(ROOT_DIR, current_dir)
            archive_path = os.path.join(ROOT_DIR, archive_dir)
            
            # Создаем директорию архива
            os.makedirs(archive_path, exist_ok=True)
            
            # Имя архива с временной меткой
            timestamp = datetime.now().strftime('%Y_%m')
            archive_filename = f"historical_data_{timestamp}.tar.gz"
            archive_file_path = os.path.join(archive_path, archive_filename)
            
            # Создаем архив через subprocess
            import subprocess
            cmd = ["tar", "-czf", archive_file_path, "-C", current_path, "."]
            subprocess.run(cmd, check=True)
            
            # Создаем маркер архивирования
            with open(os.path.join(archive_path, "last_monthly_archive.txt"), 'w') as f:
                f.write(datetime.now().strftime('%Y-%m-%d'))
            
            logger.info(f"Ежемесячное архивирование завершено: {archive_file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при архивировании: {e}")
            return False
    
    def _monthly_archive_needed(self) -> bool:
        """
        Проверка необходимости ежемесячного архивирования
        
        Returns:
            bool: True если архивирование необходимо
        """
        # Проверяем, включено ли архивирование
        if not DATA_ENGINE.get('archiving', {}).get('enabled', True):
            return False
        
        try:
            # Путь к маркеру последнего архивирования
            archive_dir = PATHS.get('DATA_ENGINE', {}).get('archive')
            if not archive_dir:
                return True
            
            archive_marker = os.path.join(ROOT_DIR, archive_dir, "last_monthly_archive.txt")
            
            # Если маркер не существует, архивирование нужно
            if not os.path.exists(archive_marker):
                return True
            
            # Проверяем дату последнего архивирования
            with open(archive_marker, 'r') as f:
                last_archive_str = f.read().strip()
            
            last_archive_date = datetime.strptime(last_archive_str, '%Y-%m-%d')
            current_date = datetime.now()
            
            # Если изменился месяц, нужно архивирование
            return (last_archive_date.year != current_date.year or 
                    last_archive_date.month != current_date.month)
        except Exception as e:
            logger.error(f"Ошибка при проверке необходимости архивирования: {e}")
            # В случае ошибки лучше провести архивирование
            return True
    
    def create_status_file(self, status: str, details: Optional[Dict] = None) -> None:
        """
        Создание файла статуса для синхронизации с другими компонентами
        
        Args:
            status: Статус выполнения ('initializing', 'in_progress', 'success', 'failed', etc.)
            details: Дополнительные детали статуса
        """
        details = details or {}
        
        try:
            # Определяем путь к файлу статуса
            status_dir = PATHS.get('DATA_ENGINE', {}).get('current')
            if not status_dir:
                status_dir = 'data/brain/data_engine/current/'
            
            status_path = os.path.join(ROOT_DIR, status_dir, "data_engine_status.json")
            
            # Данные статуса
            status_data = {
                "timestamp": datetime.now().isoformat(),
                "status": status,
                "next_step": "indicator_engine" if status == "success" else "none",
                "details": {
                    **details,
                    "execution_time": (datetime.now() - self.start_time).total_seconds()
                }
            }
            
            # Создаем временный файл
            temp_dir = PATHS.get('DATA_ENGINE', {}).get('temp')
            if not temp_dir:
                temp_dir = 'data/brain/data_engine/temp/'
            
            temp_path = os.path.join(ROOT_DIR, temp_dir, f"status_{int(time.time())}.json")
            
            # Создаем директории, если их нет
            os.makedirs(os.path.dirname(status_path), exist_ok=True)
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            
            # Записываем во временный файл
            with open(temp_path, "w") as f:
                json.dump(status_data, f, indent=4)
            
            # Перемещаем на место
            shutil.move(temp_path, status_path)
            
            logger.info(f"Создан файл статуса: {status}")
            
            # Отправляем уведомление в Telegram
            self._send_telegram_notification(status, details)
        except Exception as e:
            logger.error(f"Ошибка при создании статус-файла: {e}")
    
    def _send_telegram_notification(self, status: str, details: Optional[Dict] = None):
        """
        Отправляет статус выполнения в Telegram
        
        Args:
            status: Статус (success, partial_success, failed, error)
            details: Детали выполнения (опционально)
        """
        try:
            details = details or {}
            
            # Формируем основное сообщение
            message = f"<b>📊 Data Engine:</b> {status.upper()}\n"
            execution_time = (datetime.now() - self.start_time).total_seconds()
            message += f"<i>Время выполнения: {self._format_time(execution_time)}</i>\n\n"
            
            # Разные блоки информации в зависимости от статуса
            if status == "success":
                # Полный успех: все монеты обработаны
                message += f"✅ <b>Успешно обработано монет:</b> {details.get('processed_symbols', 0)}\n"
                message += f"✅ <b>Успешных монет:</b> {details.get('successful_symbols', 0)}\n"
                
                # Информация о таймфреймах
                if 'timeframes' in details:
                    message += "\n📈 <b>Данные по таймфреймам:</b>\n"
                    for tf, tf_data in details.get('timeframes', {}).items():
                        success_count = tf_data.get('success', 0)
                        total_count = tf_data.get('count', 0)
                        
                        # Добавляем эмодзи для разных таймфреймов
                        tf_emoji = "🟣" if tf == '7d' else "🔴" if tf == '24h' else "🟠" if tf == '4h' else "🟡" if tf == '1h' else "🟢" if tf == '30m' else "🔵" if tf == '15m' else "🟤"
                        
                        message += f"{tf_emoji} <b>{tf}:</b> {success_count}/{total_count} монет обработано\n"
                
                # Информация о стаканах
                orderbook_success = 0
                orderbook_failed = 0
                
                # Посчитаем статистику по стаканам из деталей
                for tf, tf_data in details.get('timeframes', {}).items():
                    if 'orderbook_updated' in tf_data:
                        if tf_data['orderbook_updated']:
                            orderbook_success += 1
                        else:
                            orderbook_failed += 1
                
                if orderbook_success > 0 or orderbook_failed > 0:
                    message += f"\n📊 <b>Обновление стаканов:</b>\n"
                    message += f"✅ Успешно: {orderbook_success}\n"
                    message += f"❌ Ошибки: {orderbook_failed}\n"
                    
                # Информация об инкрементальном обновлении
                incremental_updates = 0
                first_time_loaded = 0
                
                # Подсчет статистики инкрементальных обновлений из деталей
                for symbol_data in details.get('symbols_data', []):
                    status = symbol_data.get('status', '')
                    if status == 'updated':
                        incremental_updates += 1
                    elif status == 'new':
                        first_time_loaded += 1
                
                if incremental_updates > 0 or first_time_loaded > 0:
                    message += f"\n🔄 <b>Инкрементальные обновления:</b>\n"
                    message += f"🆕 Впервые загружено: {first_time_loaded} монет\n"
                    message += f"🔁 Обновлено: {incremental_updates} монет\n"
                    
                # Добавляем информацию о системе
                try:
                    memory_usage = psutil.virtual_memory()
                    message += f"\n🖥 <b>Система:</b>\n"
                    message += f"CPU: {psutil.cpu_percent()}%\n"
                    message += f"RAM: {memory_usage.percent}% " \
                            f"({memory_usage.used / (1024**3):.1f} GB / {memory_usage.total / (1024**3):.1f} GB)\n"
                except Exception as system_error:
                    logger.debug(f"Не удалось получить данные о системе: {system_error}")
                    
            elif status == "partial_success":
                # Частичный успех: некоторые монеты обработаны успешно, некоторые с ошибками
                message += f"✅ <b>Успешно обработано:</b> {details.get('successful_symbols', 0)} монет\n"
                message += f"❌ <b>С ошибками:</b> {details.get('failed_symbols', 0)} монет\n\n"
                
                # Добавляем список монет с ошибками (если есть)
                if 'failed_symbols_list' in details and details['failed_symbols_list']:
                    failed_list = details['failed_symbols_list']
                    if len(failed_list) > 10:
                        failed_str = ", ".join(failed_list[:10]) + f" и еще {len(failed_list) - 10}"
                    else:
                        failed_str = ", ".join(failed_list)
                    message += f"<b>Проблемные монеты:</b> {failed_str}\n\n"
                
                # Информация о таймфреймах
                if 'timeframes' in details:
                    message += "📈 <b>Данные по таймфреймам:</b>\n"
                    for tf, tf_data in details.get('timeframes', {}).items():
                        success_count = tf_data.get('success', 0)
                        total_count = tf_data.get('count', 0)
                        success_percent = (success_count / total_count * 100) if total_count > 0 else 0
                        
                        # Эмодзи для таймфреймов
                        tf_emoji = "🟣" if tf == '7d' else "🔴" if tf == '24h' else "🟠" if tf == '4h' else "🟡" if tf == '1h' else "🟢" if tf == '30m' else "🔵" if tf == '15m' else "🟤"
                        
                        message += f"{tf_emoji} <b>{tf}:</b> {success_count}/{total_count} ({success_percent:.1f}%)\n"
                
                # Добавляем информацию о системе
                try:
                    memory_usage = psutil.virtual_memory()
                    message += f"\n🖥 <b>Система:</b>\n"
                    message += f"CPU: {psutil.cpu_percent()}%\n"
                    message += f"RAM: {memory_usage.percent}% " \
                            f"({memory_usage.used / (1024**3):.1f} GB / {memory_usage.total / (1024**3):.1f} GB)\n"
                except Exception as system_error:
                    logger.debug(f"Не удалось получить данные о системе: {system_error}")
                    
            elif status in ["failed", "error"]:
                # Полный провал: все монеты не обработаны или произошла ошибка
                message += f"❌ <b>Ошибка:</b> {details.get('error', 'Неизвестная ошибка')}\n"
                message += f"<b>Успешно:</b> {details.get('successful_symbols', 0)}, "
                message += f"<b>С ошибками:</b> {details.get('failed_symbols', 0)}\n"
                
                if 'traceback' in details:
                    # Если есть трассировка ошибки, добавляем её (урезанную)
                    trace = details.get('traceback', '')
                    if len(trace) > 500:
                        trace = trace[:497] + "..."
                    message += f"\n<code>{trace}</code>\n"
                    
                # Добавляем информацию о системе даже при ошибке
                try:
                    memory_usage = psutil.virtual_memory()
                    message += f"\n🖥 <b>Система:</b>\n"
                    message += f"CPU: {psutil.cpu_percent()}%\n"
                    message += f"RAM: {memory_usage.percent}% " \
                            f"({memory_usage.used / (1024**3):.1f} GB / {memory_usage.total / (1024**3):.1f} GB)\n"
                except Exception:
                    pass
            
            # Максимальная длина сообщения в Telegram - 4096 символов
            if len(message) > 4000:
                message = message[:3997] + "..."
            
            # Отправляем сообщение с помощью функции из brain_logger
            send_telegram(message, priority="high" if status in ["failed", "error"] else "normal")
            
            logger.info(f"Уведомление о статусе {status} отправлено в Telegram")
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления в Telegram: {e}")
            logger.debug(f"Трассировка: {traceback.format_exc()}")
    
    def _format_time(self, seconds: float) -> str:
        """
        Форматирование времени выполнения в человекочитаемый вид
        
        Args:
            seconds: Время в секундах
            
        Returns:
            str: Форматированное время (часы, минуты, секунды)
        """
        # Преобразуем в целые части
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Формируем строку с учетом ненулевых значений
        if hours > 0:
            return f"{hours}ч {minutes:02d}м {seconds:02d}с"
        elif minutes > 0:
            return f"{minutes}м {seconds:02d}с"
        else:
            return f"{seconds}с"
    
    def run(self) -> bool:
        """
        Основной метод запуска обновления исторических данных
        
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        run_start_time = time.time()
        
        logger.info("Запуск обновления исторических данных")
        
        try:
            # Обновляем статус
            self.create_status_file("in_progress")
            
            # Проверяем необходимость архивирования
            if DATA_ENGINE.get('archiving', {}).get('enabled', True):
                self.archive_monthly()
            
            # Получаем список символов для обработки
            symbols = self.get_filtered_symbols()
            
            if not symbols:
                error_msg = "Не найдены монеты для обработки"
                logger.error(error_msg)
                self.create_status_file("failed", {"error": error_msg})
                return False
            
            # ОПТИМИЗИРОВАННАЯ ФИЛЬТРАЦИЯ: создаём сеты недоступных монет для быстрой проверки
            unavailable_pairs = {}
            for exchange_name in self.exchange_priority:
                if exchange_name in self.exchanges:
                    exchange = self.exchanges[exchange_name]
                    if hasattr(exchange, 'get_unavailable_pairs'):
                        # Собираем базовые символы из полных имён пар
                        unavailable_pairs[exchange_name] = {
                            pair.replace(exchange.default_pair, '') for pair in exchange.get_unavailable_pairs()
                        }
            
            # Фильтруем символы, которые недоступны на ВСЕХ биржах
            filtered_symbols = []
            skipped_count = 0
            for symbol in symbols:
                # Монета недоступна, если она в списке недоступных на ВСЕХ биржах
                unavailable = all(
                    symbol in unavailable_pairs.get(exchange_name, set()) 
                    for exchange_name in self.exchange_priority
                    if exchange_name in self.exchanges
                )
                
                if not unavailable:
                    filtered_symbols.append(symbol)
                else:
                    skipped_count += 1
            
            if skipped_count > 0:
                logger.info(f"Пропущено {skipped_count} монет, недоступных на всех биржах")
            
            # Обновляем список для обработки
            symbols = filtered_symbols
            
            logger.info(f"Начало обработки {len(symbols)} монет")
            
            # Запоминаем время начала для расчета производительности
            processing_start_time = time.time()
            
            # Распределяем задачи по пакетам и обрабатываем их параллельно
            results = self.thread_manager.distribute_tasks(
                symbols, self.process_symbol, batch_size=self.batch_size
            )
            
            # Время обработки всех монет
            processing_time = time.time() - processing_start_time
            # Сохраняем кэш недоступных пар
            self._save_unavailable_pairs_cache()
   
            # Разделяем результаты на успешные и неуспешные
            successful = [r for r in results if r.get('success', False)]
            failed = [r for r in results if not r.get('success', False)]
            
            # Собираем статистику по таймфреймам
            timeframes_stats = {}
            
            # Собираем статистику по стаканам и инкрементальным обновлениям
            orderbooks_stats = {
                "success": 0,
                "failed": 0
            }
            
            incremental_updates = {
                "updated": 0,  # Инкрементально обновлено
                "new": 0,      # Загружено впервые
                "skipped": 0   # Пропущено (актуально)
            }
            
            # Сбор расширенной статистики для телеграм-уведомлений
            symbols_data = []
            
            # Статистика по времени выполнения
            execution_times = []
            
            for result in successful:
                # Сбор времени обработки монеты
                exec_time = result.get('execution_time', 0)
                if exec_time > 0:
                    execution_times.append((result.get('symbol', 'Unknown'), exec_time))
                
                # Сбор статистики по таймфреймам
                for tf, tf_result in result.get('timeframes_results', {}).items():
                    if tf not in timeframes_stats:
                        timeframes_stats[tf] = {'count': 0, 'success': 0, 'failed': 0}
                    
                    if tf_result.get('success', False):
                        timeframes_stats[tf]['success'] += 1
                    else:
                        timeframes_stats[tf]['failed'] += 1
                    
                    timeframes_stats[tf]['count'] += 1
                    
                    # Сбор статистики по стаканам
                    if tf_result.get('orderbook_updated', False):
                        orderbooks_stats["success"] += 1
                    else:
                        orderbooks_stats["failed"] += 1
                    
                    # Сбор статистики по инкрементальным обновлениям
                    status = tf_result.get('status', '')
                    if status == 'updated':
                        incremental_updates["updated"] += 1
                    elif status == 'skipped':
                        incremental_updates["skipped"] += 1
                    elif status == 'new':
                        incremental_updates["new"] += 1
                
                # Сбор детальной информации по монете
                symbol_data = {
                    "symbol": result.get('symbol', 'Unknown'),
                    "status": "success" if result.get('success', False) else "failed",
                    "execution_time": result.get('execution_time', 0),
                    "timeframes_success": result.get('timeframes_success', 0),
                    "timeframes_total": result.get('timeframes_total', 0)
                }
                symbols_data.append(symbol_data)
            
            # Добавляем информацию о монетах с ошибками
            for result in failed:
                symbol_data = {
                    "symbol": result.get('symbol', 'Unknown'),
                    "status": "failed",
                    "error": result.get('message', 'Неизвестная ошибка'),
                    "execution_time": result.get('execution_time', 0)
                }
                symbols_data.append(symbol_data)
            
            # Статистика по времени выполнения
            performance_stats = {}
            if execution_times:
                execution_times.sort(key=lambda x: x[1])  # Сортируем по времени
                
                # Среднее время
                avg_time = sum(time for _, time in execution_times) / len(execution_times)
                
                # Самая быстрая и самая медленная монета
                fastest_coin, min_time = execution_times[0]
                slowest_coin, max_time = execution_times[-1]
                
                performance_stats = {
                    "avg_coin_time": avg_time,
                    "min_coin_time": min_time,
                    "max_coin_time": max_time,
                    "fastest_coin": fastest_coin,
                    "slowest_coin": slowest_coin,
                    "coins_per_minute": 60 / avg_time if avg_time > 0 else 0
                }
            
            # Общее время выполнения
            total_time = time.time() - run_start_time
            
            # Сборка деталей для статус-файла
            common_details = {
                "processed_symbols": len(symbols),
                "successful_symbols": len(successful),
                "failed_symbols": len(failed),
                "execution_time": total_time,
                "processing_time": processing_time,
                "timeframes": timeframes_stats,
                "orderbooks": orderbooks_stats,
                "incremental_updates": incremental_updates,
                "symbols_data": symbols_data,
                "performance": performance_stats
            }
            
            # Определяем статус и создаем статус-файл
            if not failed:
                # Полный успех
                self.create_status_file("success", common_details)
                
                logger.info(f"Обновление завершено успешно: {len(successful)} монет за {self._format_time(total_time)}")
                record_execution_time("run_success", None, total_time)
                
                return True
            elif successful:
                # Частичный успех - добавляем список монет с ошибками
                partial_details = {
                    **common_details,
                    "failed_symbols_list": [r.get('symbol', 'Unknown') for r in failed]
                }
                
                self.create_status_file("partial_success", partial_details)
                
                logger.warning(f"Частичный успех: {len(successful)} успешно, {len(failed)} с ошибками")
                record_execution_time("run_partial", None, total_time)
                
                return True
            else:
                # Полный провал
                failed_details = {
                    **common_details,
                    "failed_symbols_list": [r.get('symbol', 'Unknown') for r in failed]
                }
                
                self.create_status_file("failed", failed_details)
                
                logger.error(f"Все {len(failed)} монет обработаны с ошибками")
                record_execution_time("run_failed", None, total_time)
                
                return False
        except Exception as e:
            # Даже при ошибке сохраняем кэш недоступных пар
            self._save_unavailable_pairs_cache()
            # Запись времени при критической ошибке
            run_time = time.time() - run_start_time
            record_execution_time("run_error", None, run_time)
            
            # Получаем трассировку для лога и телеграм-уведомления
            error_traceback = traceback.format_exc()
            
            logger.error(f"Критическая ошибка при обновлении данных: {e}")
            logger.debug(error_traceback)
            
            # Добавляем трассировку для телеграм-уведомления
            self.create_status_file("failed", {
                "error": str(e),
                "traceback": error_traceback
            })
            
            return False
        
# ===========================================================================
# БЛОК 10: ФУНКЦИИ ЗАПУСКА СКРИПТА
# ===========================================================================
def parse_arguments():
    """Разбор аргументов командной строки"""
    parser = argparse.ArgumentParser(description='DataEngine для обновления исторических данных')
    
    # Режим тестирования
    parser.add_argument('--test', action='store_true', help='Тестовый режим с ограниченным набором монет')
    parser.add_argument('--symbols', nargs='+', help='Символы монет для обработки в тестовом режиме')
    
    # Выбор таймфреймов
    parser.add_argument('--timeframes', nargs='+', 
                      choices=['5m', '15m', '30m', '1h', '4h', '24h', '7d', 'all'], 
                      default=['all'], 
                      help='Таймфреймы для обработки')
    
    # Настройки многопоточности
    parser.add_argument('--batch-size', type=int, help='Размер пакета монет для обработки')
    parser.add_argument('--workers', type=int, help='Количество рабочих потоков')
    parser.add_argument('--no-dynamic', action='store_true', help='Отключить динамическую настройку потоков')
    
    # Режимы запуска
    parser.add_argument('--check', action='store_true', help='Только проверка наличия данных без обновления')
    parser.add_argument('--archive', action='store_true', help='Запуск ежемесячного архивирования')
    parser.add_argument('--verbose', action='store_true', help='Подробный вывод')
    
    # Выбор биржи
    parser.add_argument('--exchange', nargs='+', 
                      choices=['binance', 'mexc', 'all'], 
                      default=['all'], 
                      help='Биржи для сбора данных')
    
    # Включение/отключение стаканов
    parser.add_argument('--no-orderbooks', action='store_true', help='Отключить сбор данных стаканов')
    
    return parser.parse_args()

@log_execution_time(logger, 'data_engine')
def main():
    """Основная функция запуска скрипта"""
    script_start_time = time.time()
    
    # Разбор аргументов
    args = parse_arguments()
    
    # Настройка уровня логирования
    if args.verbose:
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Инициализация Data Engine
    engine = DataEngine(
        test_mode=args.test,
        test_symbols=args.symbols
    )
    
    # Настройка таймфреймов
    if 'all' not in args.timeframes:
        # Сначала отключаем все таймфреймы
        for timeframe in engine.enabled_timeframes:
            engine.enabled_timeframes[timeframe] = False
        
        # Затем включаем только указанные
        for timeframe in args.timeframes:
            engine.enabled_timeframes[timeframe] = True
    
    # Настройка бирж
    if 'all' not in args.exchange:
        # Устанавливаем список активных бирж, а не просто приоритет
        engine.exchange_priority = args.exchange
        
        # Удаляем из словаря бирж те, которые не в списке приоритета
        active_exchanges = {}
        for exchange_name in args.exchange:
            if exchange_name in engine.exchanges:
                active_exchanges[exchange_name] = engine.exchanges[exchange_name]
        
        engine.exchanges = active_exchanges
    
    # Настройка размера пакета
    if args.batch_size:
        engine.batch_size = args.batch_size
    
    # Настройка многопоточности
    if args.workers:
        engine.max_workers = args.workers
        engine.thread_manager.max_workers = args.workers
    
    if args.no_dynamic:
        engine.thread_manager.dynamic_workers = False
    
    # Настройка стаканов
    if args.no_orderbooks:
        engine.orderbooks_enabled = False
    
    # Режим архивирования
    if args.archive:
        logger.info("Запуск ежемесячного архивирования")
        engine.archive_monthly()
        return
    
    # Режим проверки без обновления
    if args.check:
        logger.info("Режим проверки данных без обновления")
        symbols = engine.get_filtered_symbols()
        
        if symbols:
            # Проверяем только первую монету
            sample_symbol = symbols[0]
            
            for timeframe in engine.enabled_timeframes:
                if engine.enabled_timeframes[timeframe]:
                    data = engine.load_historical_data(sample_symbol, timeframe)
                    
                    if data:
                        first_data = sorted(data, key=lambda x: x.get('time_close', ''))[:1]
                        first_date = engine.get_last_data_date(first_data)
                        last_date = engine.get_last_data_date(data)
                        
                        logger.info(f"Таймфрейм {timeframe} для {sample_symbol}: "
                                   f"{len(data)} записей, от {first_date} до {last_date}")
                    else:
                        logger.info(f"Таймфрейм {timeframe} для {sample_symbol}: данные отсутствуют")
        return
    
    # Запуск обновления исторических данных
    success = engine.run()
    
    # Запись общего времени выполнения
    script_time = time.time() - script_start_time
    record_execution_time("main_total", None, script_time)
    
    # Выход с соответствующим кодом
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
