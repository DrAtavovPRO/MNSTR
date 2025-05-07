#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль Indicator Engine для проекта MONSTER Trading System.
"""

# Стандартные библиотеки Python
import os
import sys
import time
import json
import logging
import traceback
import functools
import concurrent.futures
from typing import Dict, List, Tuple, Optional, Union, Any, Set
from abc import ABC, abstractmethod

# Определение путей для корректного импорта
# Получаем абсолютный путь к текущему файлу
current_file = os.path.abspath(__file__)
# Путь к директории brain
brain_dir = os.path.dirname(current_file)
# Путь к директории core
core_dir = os.path.dirname(brain_dir)
# Путь к директории scripts
scripts_dir = os.path.dirname(core_dir)
# Путь к корневой директории проекта
project_root = os.path.dirname(scripts_dir)

# Добавляем корневую директорию проекта в sys.path
sys.path.insert(0, project_root)

# Печатаем пути для отладки
print(f"Текущий файл: {current_file}")
print(f"Корневая директория проекта: {project_root}")
print(f"Пути Python: {sys.path}")

# Импорт научных библиотек с проверкой их наличия
try:
    import pandas as pd
    import numpy as np
    from scipy import signal
    from scipy.signal import argrelextrema
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("ОШИБКА: pandas, numpy или scipy не установлены!")
    sys.exit(1)

# Импорт компонентов системы с обработкой возможных ошибок
try:
    # Импорт утилит логирования и мониторинга
    sys.path.append(os.path.join(project_root, 'scripts'))
    from utils.brain_logger import get_logger, send_telegram, log_execution_time
    from utils.monitor_system import monitor_system
    
    # Импорт конфигурационных параметров
    from config.brain.config import (
        # Базовые пути и настройки
        PATHS,
        TIMEFRAMES,
        PROCESSING,
        
        # Параметры для Indicator Engine
        INDICATOR_ENGINE,
        
        # Настройки индикаторов и паттернов
        INDICATORS,
        PATTERNS,
        DIVERGENCES,
        
        # Дополнительные настройки для торговых сигналов
        ORDERBOOK_ANALYSIS
    )
    
    LOGGER_IMPORTED = True
    print("Успешно импортированы все необходимые модули!")
    
except ImportError as e:
    LOGGER_IMPORTED = False
    import logging
    logging.basicConfig(level=logging.ERROR)
    log_console = logging.getLogger("bootstrap")
    log_console.error(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось импортировать необходимые модули: {e}")
    log_console.error(f"Детали исключения: {traceback.format_exc()}")
    log_console.error("Проверьте настройки путей и наличие всех зависимостей")
    
    # Базовые функции логирования для диагностики
    def get_logger(name, component='default'):
        logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s'))
        logger.addHandler(handler)
        return logger
    
    def send_telegram(message, priority="normal"):
        print(f"[TELEGRAM {priority}] {message}")
    
    def log_execution_time(logger=None, component='default', level=logging.INFO):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    # Выводим более подробную диагностику
    print("Пути Python:")
    for p in sys.path:
        print(f"  - {p}")
    
    print("\nПроверка наличия файлов:")
    paths_to_check = [
        os.path.join(project_root, 'scripts', 'utils', 'brain_logger.py'),
        os.path.join(project_root, 'scripts', 'utils', 'monitor_system.py'),
        os.path.join(project_root, 'config', 'brain', 'config.py')
    ]
    
    for path in paths_to_check:
        exists = os.path.exists(path)
        print(f"  - {path}: {'Существует' if exists else 'НЕ существует'}")
    
    # Завершаем выполнение с кодом ошибки
    sys.exit(1)

# Настройка логгера с указанием компонента 'indicator_engine'
logger = get_logger("indicator_engine", component='indicator_engine')
print("Логгер успешно настроен!")

# ============================================================
# АБСТРАКТНЫЕ КЛАССЫ И ИНТЕРФЕЙСЫ
# ============================================================

class BaseIndicator(ABC):
    """
    Базовый абстрактный класс для всех индикаторов.
    Каждый индикатор должен наследоваться от этого класса и реализовать методы calculate и generate_signals.
    """
    
    def __init__(self, params: Dict = None, name: str = None):
        """
        Инициализирует индикатор.
        
        Args:
            params: Словарь с параметрами индикатора
            name: Имя индикатора (если не указано, используется имя класса)
        """
        self.params = params or {}
        self.name = name or self.__class__.__name__
        
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает индикатор и добавляет его в DataFrame.
        
        Args:
            df: DataFrame с данными (OHLCV)
            
        Returns:
            DataFrame с добавленными значениями индикатора
        """
        pass
    
    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> List[Dict]:
        """
        Генерирует торговые сигналы на основе индикатора.
        
        Args:
            df: DataFrame с данными и рассчитанным индикатором
            
        Returns:
            Список словарей с сигналами
        """
        pass
    
    def get_cache_key(self, symbol: str, timeframe: str) -> str:
        """
        Создает уникальный ключ для кеширования результатов.
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            Строка-ключ для кеширования
        """
        param_hash = hash(frozenset(self.params.items())) if self.params else 0
        return f"{symbol}_{timeframe}_{self.name}_{param_hash}"


class PatternDetector(ABC):
    """
    Базовый абстрактный класс для детекторов паттернов.
    Каждый детектор паттернов должен наследоваться от этого класса и реализовать метод detect.
    """
    
    def __init__(self, params: Dict = None, name: str = None):
        """
        Инициализирует детектор паттернов.
        
        Args:
            params: Словарь с параметрами детектора
            name: Имя детектора (если не указано, используется имя класса)
        """
        self.params = params or {}
        self.name = name or self.__class__.__name__
        
    @abstractmethod
    def detect(self, df: pd.DataFrame) -> List[Dict]:
        """
        Обнаруживает паттерны в DataFrame.
        
        Args:
            df: DataFrame с данными (OHLCV)
            
        Returns:
            Список словарей с паттернами
        """
        pass
    
    def get_cache_key(self, symbol: str, timeframe: str) -> str:
        """
        Создает уникальный ключ для кеширования результатов.
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            Строка-ключ для кеширования
        """
        param_hash = hash(frozenset(self.params.items())) if self.params else 0
        return f"{symbol}_{timeframe}_{self.name}_{param_hash}"


# ============================================================
# УТИЛИТЫ КЕШИРОВАНИЯ, ХРАНЕНИЯ И ОБРАБОТКИ
# ============================================================

class DataCache:
    """
    Класс для кеширования результатов расчетов.
    Поддерживает кеширование в памяти и опционально в Redis.
    """
    
    def __init__(self, use_redis: bool = False, redis_config: Dict = None):
        """
        Инициализирует кеш данных.
        
        Args:
            use_redis: Использовать ли Redis для кеширования
            redis_config: Конфигурация Redis (хост, порт, база данных)
        """
        self.memory_cache = {}
        self.use_redis = use_redis
        
        if use_redis:
            try:
                import redis
                redis_config = redis_config or {}
                # Подключение к Redis
                self.redis_client = redis.Redis(
                    host=redis_config.get('host', 'localhost'),
                    port=redis_config.get('port', 6379),
                    db=redis_config.get('db', 0)
                )
                logger.info("Redis подключен успешно")
            except ImportError:
                logger.warning("Модуль redis не установлен. Установите его с помощью pip install redis")
                self.use_redis = False
            except Exception as e:
                logger.error(f"Ошибка при подключении к Redis: {e}")
                self.use_redis = False
    
    def get(self, key: str) -> Any:
        """
        Получает данные из кеша.
        
        Args:
            key: Ключ для поиска в кеше
            
        Returns:
            Значение из кеша или None, если ключа нет
        """
        # Сначала проверяем в памяти
        if key in self.memory_cache:
            return self.memory_cache[key]
        
        # Затем проверяем в Redis, если он используется
        if self.use_redis:
            try:
                import pickle
                data = self.redis_client.get(key)
                if data:
                    return pickle.loads(data)
            except Exception as e:
                logger.error(f"Ошибка при получении данных из Redis: {e}")
        
        return None
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """
        Сохраняет данные в кеш.
        
        Args:
            key: Ключ для сохранения в кеше
            value: Значение для сохранения
            ttl: Время жизни в секундах (только для Redis)
        """
        # Сохраняем в памяти
        self.memory_cache[key] = value
        
        # Сохраняем в Redis, если он используется
        if self.use_redis:
            try:
                import pickle
                self.redis_client.set(key, pickle.dumps(value))
                if ttl:
                    self.redis_client.expire(key, ttl)
            except Exception as e:
                logger.error(f"Ошибка при сохранении данных в Redis: {e}")


class DataStorage:
    """
    Класс для работы с хранилищем данных.
    Поддерживает сохранение в JSON и опционально в Parquet.
    """
    
    def __init__(self, base_path: str, use_parquet: bool = False):
        """
        Инициализирует хранилище данных.
        
        Args:
            base_path: Базовый путь к данным
            use_parquet: Использовать ли Parquet для хранения данных
        """
        self.base_path = base_path
        self.use_parquet = use_parquet
        
        if use_parquet:
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
                self.pa = pa
                self.pq = pq
            except ImportError:
                logger.warning("Модули pyarrow и/или pyarrow.parquet не установлены. Установите их с помощью pip install pyarrow")
                self.use_parquet = False
    
    def save_indicators(self, symbol: str, timeframe: str, data: Dict) -> None:
        """
        Сохраняет результаты расчета индикаторов.
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            data: Словарь с результатами расчета индикаторов
        """
        # Создаем директорию, если она не существует
        indicators_dir = os.path.join(self.base_path, "indicators", "current", timeframe)
        os.makedirs(indicators_dir, exist_ok=True)
        
        # Путь к файлу результатов
        file_path = os.path.join(indicators_dir, f"{symbol}_{timeframe}")
        
        if self.use_parquet:
            try:
                # Преобразование в формат, подходящий для Parquet
                flat_data = self._flatten_dict(data)
                table = self.pa.Table.from_pydict(flat_data)
                self.pq.write_table(table, f"{file_path}.parquet")
            except Exception as e:
                logger.error(f"Ошибка при сохранении данных в Parquet: {e}")
                # Если возникла ошибка, сохраняем в JSON как запасной вариант
                self._save_to_json(f"{file_path}.json", data)
        else:
            # Сохранение в JSON
            self._save_to_json(f"{file_path}.json", data)
    
    def _save_to_json(self, file_path: str, data: Dict) -> None:
        """
        Сохраняет данные в JSON файл.
        
        Args:
            file_path: Путь к файлу
            data: Данные для сохранения
        """
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка при сохранении в JSON: {e}")
    
    def load_indicators(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """
        Загружает сохраненные результаты индикаторов.
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            Словарь с результатами расчета индикаторов или None, если файл не найден
        """
        # Путь к файлу результатов
        indicators_dir = os.path.join(self.base_path, "indicators", "current", timeframe)
        file_path = os.path.join(indicators_dir, f"{symbol}_{timeframe}")
        
        try:
            if self.use_parquet and os.path.exists(f"{file_path}.parquet"):
                # Загрузка из Parquet
                table = self.pq.read_table(f"{file_path}.parquet")
                flat_dict = {col: table[col].to_pylist() for col in table.column_names}
                return self._unflatten_dict(flat_dict)
            elif os.path.exists(f"{file_path}.json"):
                # Загрузка из JSON
                with open(f"{file_path}.json", 'r') as f:
                    return json.load(f)
            else:
                logger.debug(f"Файл с индикаторами для {symbol} ({timeframe}) не найден")
                return None
        except Exception as e:
            logger.error(f"Ошибка при загрузке индикаторов для {symbol} ({timeframe}): {e}")
            return None
    
    def _flatten_dict(self, d: Dict, parent_key: str = '') -> Dict:
        """
        Преобразует вложенный словарь в плоскую структуру для сохранения в Parquet.
        
        Args:
            d: Вложенный словарь
            parent_key: Ключ родительского элемента
            
        Returns:
            Плоский словарь с составными ключами
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _unflatten_dict(self, d: Dict) -> Dict:
        """
        Преобразует плоскую структуру обратно во вложенный словарь после загрузки из Parquet.
        
        Args:
            d: Плоский словарь с составными ключами
            
        Returns:
            Вложенный словарь
        """
        result = {}
        for key, value in d.items():
            parts = key.split('.')
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result


class ParallelProcessor:
    """
    Класс для параллельной обработки данных с использованием ThreadPoolExecutor.
    """
    
    def __init__(self, max_workers: int = None, chunk_size: int = None):
        """
        Инициализирует процессор.
        
        Args:
            max_workers: Максимальное количество рабочих потоков
            chunk_size: Размер пакета для обработки
        """
        self.max_workers = max_workers
        self.chunk_size = chunk_size
    
    def process_symbols(self, symbols: List[str], process_func, **kwargs) -> Dict:
        """
        Обрабатывает список символов параллельно.
        
        Args:
            symbols: Список символов для обработки
            process_func: Функция для обработки пакета символов
            **kwargs: Дополнительные параметры для функции обработки
            
        Returns:
            Словарь с результатами обработки
        """
        start_time = time.time()
        
        # Подготовка чанков для обработки
        if self.chunk_size and len(symbols) > self.chunk_size:
            chunks = [symbols[i:i+self.chunk_size] for i in range(0, len(symbols), self.chunk_size)]
        else:
            chunks = [symbols]
        
        results = {
            'success': True,
            'total_symbols': len(symbols),
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_signals': 0,
            'results': {}
        }
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for chunk in chunks:
                future = executor.submit(process_func, chunk, **kwargs)
                futures.append(future)
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    chunk_result = future.result()
                    
                    # Объединение результатов
                    results['successful_symbols'] += chunk_result.get('successful_symbols', 0)
                    results['failed_symbols'] += chunk_result.get('failed_symbols', 0)
                    results['total_signals'] += chunk_result.get('total_signals', 0)
                    
                    if 'results' in chunk_result:
                        results['results'].update(chunk_result['results'])
                        
                except Exception as e:
                    logger.error(f"Ошибка при обработке чанка: {e}")
                    results['success'] = False
        
        execution_time = time.time() - start_time
        results['execution_time'] = execution_time
        
        return results


# ============================================================
# РЕАЛИЗАЦИИ КОНКРЕТНЫХ ИНДИКАТОРОВ
# ============================================================

class RSI(BaseIndicator):
    """
    Индикатор Relative Strength Index (RSI).
    
    RSI измеряет скорость и изменение ценовых движений. Обычно используется для определения
    перекупленных или перепроданных условий в торговле актива.
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализирует индикатор RSI.
        
        Args:
            params: Словарь с параметрами индикатора
                - period: Период расчета (обычно 14)
                - overbought: Уровень перекупленности (обычно 70)
                - oversold: Уровень перепроданности (обычно 30)
        """
        super().__init__(params)
        self.period = self.params.get('period', 14)
        self.overbought = self.params.get('overbought', 70)
        self.oversold = self.params.get('oversold', 30)
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает RSI и добавляет его в DataFrame.
        
        Формула:
            RSI = 100 - (100 / (1 + RS))
            где RS = Средний прирост / Средние потери
        
        Args:
            df: DataFrame с данными (должен содержать столбец 'close')
            
        Returns:
            DataFrame с добавленным столбцом 'rsi'
        """
        if 'close' not in df.columns:
            raise ValueError("DataFrame должен содержать столбец 'close' для расчета RSI")
        
        try:
            # Оптимизированный расчет RSI с использованием векторизации Pandas
            close_delta = df['close'].diff()
            
            # Получаем положительные и отрицательные изменения
            up = close_delta.where(close_delta > 0, 0)
            down = -close_delta.where(close_delta < 0, 0)
            
            # Рассчитываем среднее значение положительных и отрицательных изменений
            avg_gain = up.ewm(com=self.period-1, min_periods=self.period).mean()
            avg_loss = down.ewm(com=self.period-1, min_periods=self.period).mean()
            
            # Защита от деления на ноль
            avg_loss = avg_loss.where(avg_loss != 0, 0.00001)
            
            # Рассчитываем relative strength (RS)
            rs = avg_gain / avg_loss
            
            # Рассчитываем RSI
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # Заменяем NaN на None для JSON-сериализации
            df['rsi'] = df['rsi'].fillna(50)  # 50 - нейтральное значение для RSI
            
            return df
        except Exception as e:
            logger.error(f"Ошибка при расчете RSI: {e}")
            # В случае ошибки добавляем пустой столбец RSI
            df['rsi'] = [None] * len(df)
            return df
    
    def generate_signals(self, df: pd.DataFrame) -> List[Dict]:
        """
        Генерирует торговые сигналы на основе RSI.
        
        Сигналы:
        - Перепроданность (RSI < oversold): возможная покупка
        - Перекупленность (RSI > overbought): возможная продажа
        
        Args:
            df: DataFrame с данными и рассчитанным RSI
            
        Returns:
            Список словарей с сигналами
        """
        signals = []
        
        if 'rsi' not in df.columns:
            logger.warning("Столбец 'rsi' не найден в DataFrame. Сигналы не сгенерированы.")
            return signals
        
        try:
            # Проверяем условия для сигналов
            # Перепроданность (признак возможной покупки)
            oversold_condition = df['rsi'] < self.oversold
            if oversold_condition.any():
                # Находим точки пересечения уровня перепроданности снизу вверх
                crosses_up = (df['rsi'].shift(1) < self.oversold) & (df['rsi'] >= self.oversold)
                
                for idx in df.index[crosses_up]:
                    signals.append({
                        'indicator': 'RSI',
                        'signal_type': 'buy',
                        'timestamp': str(idx) if not isinstance(idx, str) else idx,
                        'price': df.loc[idx, 'close'],
                        'strength': 1 - (df.loc[idx, 'rsi'] / self.oversold),  # Сила сигнала
                        'description': f'RSI пересек уровень перепроданности {self.oversold} снизу вверх'
                    })
            
            # Перекупленность (признак возможной продажи)
            overbought_condition = df['rsi'] > self.overbought
            if overbought_condition.any():
                # Находим точки пересечения уровня перекупленности сверху вниз
                crosses_down = (df['rsi'].shift(1) > self.overbought) & (df['rsi'] <= self.overbought)
                
                for idx in df.index[crosses_down]:
                    signals.append({
                        'indicator': 'RSI',
                        'signal_type': 'sell',
                        'timestamp': str(idx) if not isinstance(idx, str) else idx,
                        'price': df.loc[idx, 'close'],
                        'strength': (df.loc[idx, 'rsi'] - self.overbought) / (100 - self.overbought),  # Сила сигнала
                        'description': f'RSI пересек уровень перекупленности {self.overbought} сверху вниз'
                    })
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов RSI: {e}")
        
        return signals


class MACD(BaseIndicator):
    """
    Индикатор Moving Average Convergence Divergence (MACD).
    
    MACD - индикатор тренда, который показывает взаимосвязь между двумя скользящими средними цены.
    Он рассчитывается путем вычитания 26-периодной экспоненциальной скользящей средней (EMA) из
    12-периодной EMA. Результирующая линия - это линия MACD. 9-периодная EMA линии MACD построенная
    с использованием линии MACD является сигнальной линией.
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализирует индикатор MACD.
        
        Args:
            params: Словарь с параметрами индикатора
                - fast_period: Период быстрой EMA (обычно 12)
                - slow_period: Период медленной EMA (обычно 26)
                - signal_period: Период сигнальной линии (обычно 9)
        """
        super().__init__(params)
        self.fast_period = self.params.get('fast_period', 12)
        self.slow_period = self.params.get('slow_period', 26)
        self.signal_period = self.params.get('signal_period', 9)
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает MACD и добавляет его в DataFrame.
        
        Формула:
            MACD Line = FastEMA - SlowEMA
            Signal Line = EMA of MACD Line
            Histogram = MACD Line - Signal Line
        
        Args:
            df: DataFrame с данными (должен содержать столбец 'close')
            
        Returns:
            DataFrame с добавленными столбцами 'macd_line', 'macd_signal' и 'macd_histogram'
        """
        if 'close' not in df.columns:
            raise ValueError("DataFrame должен содержать столбец 'close' для расчета MACD")
        
        try:
            # Расчет экспоненциальных скользящих средних
            fast_ema = df['close'].ewm(span=self.fast_period, adjust=False).mean()
            slow_ema = df['close'].ewm(span=self.slow_period, adjust=False).mean()
            
            # Расчет линии MACD
            macd_line = fast_ema - slow_ema
            
            # Расчет сигнальной линии
            signal_line = macd_line.ewm(span=self.signal_period, adjust=False).mean()
            
            # Расчет гистограммы MACD
            macd_histogram = macd_line - signal_line
            
            # Добавление результатов в DataFrame
            df['macd_line'] = macd_line
            df['macd_signal'] = signal_line
            df['macd_histogram'] = macd_histogram
            
            return df
        except Exception as e:
            logger.error(f"Ошибка при расчете MACD: {e}")
            # В случае ошибки добавляем пустые столбцы MACD
            df['macd_line'] = [None] * len(df)
            df['macd_signal'] = [None] * len(df)
            df['macd_histogram'] = [None] * len(df)
            return df
    
    def generate_signals(self, df: pd.DataFrame) -> List[Dict]:
        """
        Генерирует торговые сигналы на основе MACD.
        
        Сигналы:
        - Пересечение линии MACD и сигнальной линии снизу вверх: покупка
        - Пересечение линии MACD и сигнальной линии сверху вниз: продажа
        
        Args:
            df: DataFrame с данными и рассчитанным MACD
            
        Returns:
            Список словарей с сигналами
        """
        signals = []
        
        if 'macd_line' not in df.columns or 'macd_signal' not in df.columns:
            logger.warning("Столбцы MACD не найдены в DataFrame. Сигналы не сгенерированы.")
            return signals
        
        try:
            # Проверяем пересечения MACD и сигнальной линии
            crosses_above = ((df['macd_line'].shift(1) < df['macd_signal'].shift(1)) & 
                            (df['macd_line'] > df['macd_signal'])).fillna(False)
            
            crosses_below = ((df['macd_line'].shift(1) > df['macd_signal'].shift(1)) & 
                            (df['macd_line'] < df['macd_signal'])).fillna(False)
            
            # Генерация сигналов покупки (пересечение снизу вверх)
            for idx in df.index[crosses_above]:
                # Расчет силы сигнала на основе разницы между MACD и сигнальной линией
                strength = abs(df.loc[idx, 'macd_histogram'] / df.loc[idx, 'macd_line']) if df.loc[idx, 'macd_line'] != 0 else 0.5
                strength = min(strength, 1.0)  # Ограничиваем силу сигнала до 1.0
                
                signals.append({
                    'indicator': 'MACD',
                    'signal_type': 'buy',
                    'timestamp': str(idx) if not isinstance(idx, str) else idx,
                    'price': df.loc[idx, 'close'],
                    'strength': strength,
                    'description': 'Линия MACD пересекла сигнальную линию снизу вверх'
                })
            
            # Генерация сигналов продажи (пересечение сверху вниз)
            for idx in df.index[crosses_below]:
                # Расчет силы сигнала на основе разницы между MACD и сигнальной линией
                strength = abs(df.loc[idx, 'macd_histogram'] / df.loc[idx, 'macd_line']) if df.loc[idx, 'macd_line'] != 0 else 0.5
                strength = min(strength, 1.0)  # Ограничиваем силу сигнала до 1.0
                
                signals.append({
                    'indicator': 'MACD',
                    'signal_type': 'sell',
                    'timestamp': str(idx) if not isinstance(idx, str) else idx,
                    'price': df.loc[idx, 'close'],
                    'strength': strength,
                    'description': 'Линия MACD пересекла сигнальную линию сверху вниз'
                })
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов MACD: {e}")
        
        return signals


# ============================================================
# РЕАЛИЗАЦИИ ДЕТЕКТОРОВ ПАТТЕРНОВ
# ============================================================

class PricePatternDetector(PatternDetector):
    """
    Детектор ценовых паттернов.
    
    Обнаруживает классические ценовые паттерны, такие как "Голова и плечи",
    "Двойная вершина", "Двойное дно" и др.
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализирует детектор ценовых паттернов.
        
        Args:
            params: Словарь с параметрами детектора
                - window: Окно для поиска локальных экстремумов
                - confidence_threshold: Порог уверенности для обнаружения паттернов
        """
        super().__init__(params)
        self.window = self.params.get('window', 5)
        self.confidence_threshold = self.params.get('confidence_threshold', 0.7)
        # Флаг активации детектора
        self.enabled = self.params.get('enabled', False)
    
    def detect(self, df: pd.DataFrame) -> List[Dict]:
        """
        Обнаруживает ценовые паттерны в DataFrame.
        
        Args:
            df: DataFrame с данными OHLCV
            
        Returns:
            Список словарей с паттернами
        """
        # Если детектор отключен, возвращаем пустой список
        if not self.enabled:
            return []
            
        patterns = []
        
        try:
            # Минимальное количество свечей для анализа
            if len(df) < self.window * 3:
                logger.debug("Недостаточно данных для обнаружения паттернов")
                return patterns
            
            # Определяем локальные максимумы и минимумы
            # Используем SciPy для более эффективного обнаружения
            
            # Находим локальные максимумы
            max_indices = argrelextrema(df['high'].values, np.greater, order=self.window)[0]
            
            # Находим локальные минимумы
            min_indices = argrelextrema(df['low'].values, np.less, order=self.window)[0]
            
            # Проверяем на паттерн "Голова и плечи"
            head_and_shoulders = self._detect_head_and_shoulders(df, max_indices, min_indices)
            if head_and_shoulders:
                patterns.extend(head_and_shoulders)
            
            # Проверяем на паттерн "Двойная вершина"
            double_top = self._detect_double_top(df, max_indices)
            if double_top:
                patterns.extend(double_top)
            
            # Проверяем на паттерн "Двойное дно"
            double_bottom = self._detect_double_bottom(df, min_indices)
            if double_bottom:
                patterns.extend(double_bottom)
        except Exception as e:
            logger.error(f"Ошибка при обнаружении ценовых паттернов: {e}")
        
        return patterns
    
    def _detect_head_and_shoulders(self, df: pd.DataFrame, max_indices: np.ndarray, min_indices: np.ndarray) -> List[Dict]:
        """
        Обнаруживает паттерн 'Голова и плечи'.
        
        Паттерн "Голова и плечи" состоит из трех пиков, где средний пик (голова) выше,
        чем два других пика (плечи), которые примерно на одном уровне.
        
        Args:
            df: DataFrame с данными
            max_indices: Индексы локальных максимумов
            min_indices: Индексы локальных минимумов
            
        Returns:
            Список словарей с паттернами "Голова и плечи"
        """
        patterns = []
        
        # Логика обнаружения паттерна "Голова и плечи"
        if len(max_indices) >= 3 and len(min_indices) >= 2:
            # Проверяем последовательные максимумы для классического паттерна H&S
            for i in range(len(max_indices) - 2):
                # Получаем три последовательных максимума
                left_shoulder_idx = max_indices[i]
                head_idx = max_indices[i + 1]
                right_shoulder_idx = max_indices[i + 2]
                
                # Проверяем условия паттерна:
                # 1. Голова должна быть выше обоих плеч
                # 2. Плечи должны быть примерно на одном уровне
                
                if (df['high'].iloc[head_idx] > df['high'].iloc[left_shoulder_idx] and 
                    df['high'].iloc[head_idx] > df['high'].iloc[right_shoulder_idx] and
                    abs(df['high'].iloc[left_shoulder_idx] - df['high'].iloc[right_shoulder_idx]) / 
                    df['high'].iloc[left_shoulder_idx] < 0.1):  # 10% допуск для разницы в высоте плеч
                    
                    # Находим "шею" паттерна - линию поддержки
                    # Для упрощения используем минимумы между плечами и головой
                    neck_level = min(df['low'].iloc[left_shoulder_idx:right_shoulder_idx])
                    
                    # Расчет целевой цены
                    # Классическая цель: расстояние от шеи до головы, отложенное вниз от шеи
                    price_target = neck_level - (df['high'].iloc[head_idx] - neck_level)
                    
                    # Расчет уверенности в паттерне
                    # Факторы: разница высот плеч, высота головы, четкость шеи
                    shoulder_diff = abs(df['high'].iloc[left_shoulder_idx] - df['high'].iloc[right_shoulder_idx]) / df['high'].iloc[left_shoulder_idx]
                    head_prominence = (df['high'].iloc[head_idx] - max(df['high'].iloc[left_shoulder_idx], df['high'].iloc[right_shoulder_idx])) / df['high'].iloc[head_idx]
                    
                    # Взвешенная оценка уверенности
                    confidence = 0.8 - shoulder_diff + head_prominence
                    confidence = max(0.5, min(0.95, confidence))  # Ограничиваем от 0.5 до 0.95
                    
                    # Создаем запись о паттерне
                    pattern = {
                        'type': 'price',
                        'subtype': 'Head_and_Shoulders',
                        'start_idx': int(left_shoulder_idx),
                        'end_idx': int(right_shoulder_idx),
                        'confidence': round(confidence, 2),
                        'expected_direction': 'sell',
                        'description': 'Паттерн \'Голова и плечи\' указывает на вероятный разворот вниз',
                        'support_levels': [round(neck_level, 6)],
                        'resistance_levels': [round(df['high'].iloc[head_idx], 6)],
                        'price_targets': [round(price_target, 6)]
                    }
                    
                    patterns.append(pattern)
        
        return patterns
    
    def _detect_double_top(self, df: pd.DataFrame, max_indices: np.ndarray) -> List[Dict]:
        """
        Обнаруживает паттерн 'Двойная вершина'.
        
        Паттерн "Двойная вершина" состоит из двух пиков примерно на одном уровне,
        с заметным минимумом между ними.
        
        Args:
            df: DataFrame с данными
            max_indices: Индексы локальных максимумов
            
        Returns:
            Список словарей с паттернами "Двойная вершина"
        """
        patterns = []
        
        # Логика обнаружения паттерна "Двойная вершина"
        if len(max_indices) >= 2:
            for i in range(len(max_indices) - 1):
                first_peak_idx = max_indices[i]
                second_peak_idx = max_indices[i + 1]
                
                # Проверяем, что пики достаточно разделены
                if second_peak_idx - first_peak_idx < self.window * 2:
                    continue
                
                # Проверяем, что вершины находятся на одном уровне (с небольшим допуском)
                if abs(df['high'].iloc[first_peak_idx] - df['high'].iloc[second_peak_idx]) / df['high'].iloc[first_peak_idx] < 0.05:
                    # Находим минимум между пиками
                    min_between = df['low'].iloc[first_peak_idx:second_peak_idx].min()
                    min_idx = df['low'].iloc[first_peak_idx:second_peak_idx].idxmin()
                    
                    # Проверяем глубину коррекции между пиками
                    peak_height = df['high'].iloc[first_peak_idx]
                    correction_depth = (peak_height - min_between) / peak_height
                    
                    # Требуем минимальную глубину коррекции для подтверждения паттерна
                    if correction_depth < 0.02:  # Минимум 2% коррекции
                        continue
                    
                    # Расчет целевой цены
                    # Классическая цель: расстояние от минимума до вершины, отложенное вниз от минимума
                    price_target = min_between - (peak_height - min_between)
                    
                    # Расчет уверенности в паттерне
                    # Факторы: разница высот пиков, глубина коррекции
                    peak_diff = abs(df['high'].iloc[first_peak_idx] - df['high'].iloc[second_peak_idx]) / df['high'].iloc[first_peak_idx]
                    
                    # Взвешенная оценка уверенности
                    confidence = 0.75 - peak_diff + correction_depth
                    confidence = max(0.5, min(0.95, confidence))  # Ограничиваем от 0.5 до 0.95
                    
                    # Создаем запись о паттерне
                    pattern = {
                        'type': 'price',
                        'subtype': 'Double_Top',
                        'start_idx': int(first_peak_idx),
                        'end_idx': int(second_peak_idx),
                        'confidence': round(confidence, 2),
                        'expected_direction': 'sell',
                        'description': 'Паттерн \'Двойная вершина\' указывает на вероятный разворот вниз',
                        'support_levels': [round(min_between, 6)],
                        'resistance_levels': [round(df['high'].iloc[first_peak_idx], 6)],
                        'price_targets': [round(price_target, 6)]
                    }
                    
                    patterns.append(pattern)
        
        return patterns
    
    def _detect_double_bottom(self, df: pd.DataFrame, min_indices: np.ndarray) -> List[Dict]:
        """
        Обнаруживает паттерн 'Двойное дно'.
        
        Паттерн "Двойное дно" состоит из двух минимумов примерно на одном уровне,
        с заметным максимумом между ними.
        
        Args:
            df: DataFrame с данными
            min_indices: Индексы локальных минимумов
            
        Returns:
            Список словарей с паттернами "Двойное дно"
        """
        patterns = []
        
        # Логика обнаружения паттерна "Двойное дно"
        if len(min_indices) >= 2:
            for i in range(len(min_indices) - 1):
                first_bottom_idx = min_indices[i]
                second_bottom_idx = min_indices[i + 1]
                
                # Проверяем, что минимумы достаточно разделены
                if second_bottom_idx - first_bottom_idx < self.window * 2:
                    continue
                
                # Проверяем, что дно находится на одном уровне (с небольшим допуском)
                if abs(df['low'].iloc[first_bottom_idx] - df['low'].iloc[second_bottom_idx]) / df['low'].iloc[first_bottom_idx] < 0.05:
                    # Находим максимум между минимумами
                    max_between = df['high'].iloc[first_bottom_idx:second_bottom_idx].max()
                    max_idx = df['high'].iloc[first_bottom_idx:second_bottom_idx].idxmax()
                    
                    # Проверяем высоту отскока между минимумами
                    bottom_level = df['low'].iloc[first_bottom_idx]
                    bounce_height = (max_between - bottom_level) / bottom_level
                    
                    # Требуем минимальную высоту отскока для подтверждения паттерна
                    if bounce_height < 0.02:  # Минимум 2% отскока
                        continue
                    
                    # Расчет целевой цены
                    # Классическая цель: расстояние от минимума до максимума, отложенное вверх от максимума
                    price_target = max_between + (max_between - bottom_level)
                    
                    # Расчет уверенности в паттерне
                    # Факторы: разница уровней дна, высота отскока
                    bottom_diff = abs(df['low'].iloc[first_bottom_idx] - df['low'].iloc[second_bottom_idx]) / df['low'].iloc[first_bottom_idx]
                    
                    # Взвешенная оценка уверенности
                    confidence = 0.75 - bottom_diff + bounce_height
                    confidence = max(0.5, min(0.95, confidence))  # Ограничиваем от 0.5 до 0.95
                    
                    # Создаем запись о паттерне
                    pattern = {
                        'type': 'price',
                        'subtype': 'Double_Bottom',
                        'start_idx': int(first_bottom_idx),
                        'end_idx': int(second_bottom_idx),
                        'confidence': round(confidence, 2),
                        'expected_direction': 'buy',
                        'description': 'Паттерн \'Двойное дно\' указывает на вероятный разворот вверх',
                        'support_levels': [round(df['low'].iloc[first_bottom_idx], 6)],
                        'resistance_levels': [round(max_between, 6)],
                        'price_targets': [round(price_target, 6)]
                    }
                    
                    patterns.append(pattern)
        
        return patterns


class VolumePatternDetector(PatternDetector):
    """
    Детектор объемных паттернов.
    
    Обнаруживает паттерны, связанные с объемом торгов, такие как "Объемный всплеск",
    "Поглощение объема" и др.
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализирует детектор объемных паттернов.
        
        Args:
            params: Словарь с параметрами детектора
                - window: Окно для расчета среднего объема
                - threshold: Порог для определения аномальных объемов
        """
        super().__init__(params)
        self.window = self.params.get('window', 10)
        self.threshold = self.params.get('threshold', 1.5)  # Порог для определения аномальных объемов
        # Флаг активации детектора
        self.enabled = self.params.get('enabled', False)
    
    def detect(self, df: pd.DataFrame) -> List[Dict]:
        """
        Обнаруживает объемные паттерны в DataFrame.
        
        Args:
            df: DataFrame с данными OHLCV
            
        Returns:
            Список словарей с паттернами
        """
        # Если детектор отключен, возвращаем пустой список
        if not self.enabled:
            return []
            
        patterns = []
        
        try:
            # Проверяем наличие данных об объеме
            if 'volume' not in df.columns:
                logger.warning("Столбец 'volume' не найден в DataFrame. Объемные паттерны не обнаружены.")
                return patterns
            
            # Минимальное количество свечей для анализа
            if len(df) < self.window:
                logger.debug("Недостаточно данных для обнаружения объемных паттернов")
                return patterns
            
            # Рассчитываем средний объем за указанный период
            df['avg_volume'] = df['volume'].rolling(window=self.window).mean()
            
            # Заполняем NaN значения (для первых self.window-1 строк)
            df['avg_volume'] = df['avg_volume'].fillna(df['volume'])
            
            # Определяем аномальные объемы
            df['volume_ratio'] = df['volume'] / df['avg_volume']
            
            # Отмечаем свечи с аномально высоким объемом
            high_volume = df['volume_ratio'] > self.threshold
            
            # Проверяем на паттерн "Объемный всплеск"
            volume_spikes = self._detect_volume_spikes(df, high_volume)
            if volume_spikes:
                patterns.extend(volume_spikes)
            
            # Проверяем на паттерн "Поглощение объема"
            volume_absorption = self._detect_volume_absorption(df)
            if volume_absorption:
                patterns.extend(volume_absorption)
        except Exception as e:
            logger.error(f"Ошибка при обнаружении объемных паттернов: {e}")
        
        return patterns
    
    def _detect_volume_spikes(self, df: pd.DataFrame, high_volume: pd.Series) -> List[Dict]:
        """
        Обнаруживает паттерн 'Объемный всплеск'.
        
        Паттерн "Объемный всплеск" - это свеча с объемом, значительно превышающим средний.
        Такие всплески часто сигнализируют о сильном интересе к активу и могут предшествовать
        изменению тренда.
        
        Args:
            df: DataFrame с данными
            high_volume: Серия булевых значений, указывающих на свечи с высоким объемом
            
        Returns:
            Список словарей с паттернами "Объемный всплеск"
        """
        patterns = []
        
        # Находим индексы свечей с высоким объемом
        spike_indices = df.index[high_volume]
        
        for idx in spike_indices:
            # Определяем направление движения цены
            price_change = df.loc[idx, 'close'] - df.loc[idx, 'open']
            price_change_pct = price_change / df.loc[idx, 'open']
            direction = 'up' if price_change > 0 else 'down'
            
            # Оцениваем значимость всплеска объема
            significance = df.loc[idx, 'volume_ratio']
            
            # Оцениваем силу движения цены
            price_strength = abs(price_change_pct)
            
            # Ожидаемое направление движения
            # Высокий объем + сильное движение цены = сильный сигнал в том же направлении
            # Высокий объем + слабое движение цены = возможный разворот
            expected_direction = 'buy' if direction == 'up' and price_strength > 0.01 else 'sell' if direction == 'down' and price_strength > 0.01 else 'neutral'
            
            # Рассчитываем уверенность на основе объема и движения цены
            confidence = min(significance / 5, 0.9)  # Максимум 0.9 для объемного всплеска
            
            # Если движение цены не соответствует объему, снижаем уверенность
            if price_strength < 0.005:  # Менее 0.5% движения цены
                confidence *= 0.5
            
            # Создаем запись о паттерне
            pattern = {
                'type': 'volume',
                'subtype': 'Volume_Spike',
                'index': int(idx) if not isinstance(idx, str) else idx,
                'confidence': round(confidence, 2),
                'expected_direction': expected_direction,
                'description': f'Обнаружен всплеск объема ({significance:.2f}x от среднего) при движении цены {direction}',
                'volume_ratio': round(significance, 2),
                'price_change_percent': round(price_change_pct * 100, 2)
            }
            
            patterns.append(pattern)
        
        return patterns
    
    def _detect_volume_absorption(self, df: pd.DataFrame) -> List[Dict]:
        """
        Обнаруживает паттерн 'Поглощение объема'.
        
        Паттерн "Поглощение объема" - это ситуация, когда после периода с высоким объемом
        и малым движением цены следует значительное движение цены. Это может указывать
        на то, что крупные игроки накапливали или распределяли позиции.
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Список словарей с паттернами "Поглощение объема"
        """
        patterns = []
        
        # Требуется не менее 4 свечей для анализа
        if len(df) < 4:
            return patterns
        
        # Логика определения поглощения объема
        # Пример: Две свечи с высоким объемом, но малым движением цены, за которыми следует свеча с большим движением цены
        for i in range(len(df) - 3):
            # Проверяем объемы трех последовательных свечей
            vol1 = df.iloc[i]['volume']
            vol2 = df.iloc[i+1]['volume']
            vol3 = df.iloc[i+2]['volume']
            
            # Среднй объем для нормализации
            avg_vol = df['volume'].iloc[max(0, i-self.window):i].mean() if i > 0 else df['volume'].iloc[0]
            
            # Проверяем, что первые две свечи имеют объем выше среднего
            if vol1 < avg_vol * 1.2 or vol2 < avg_vol * 1.2:
                continue
            
            # Проверяем диапазоны цен
            range1 = abs(df.iloc[i]['high'] - df.iloc[i]['low'])
            range2 = abs(df.iloc[i+1]['high'] - df.iloc[i+1]['low'])
            range3 = abs(df.iloc[i+2]['high'] - df.iloc[i+2]['low'])
            
            # Проверяем "эффективность" объема (соотношение объема к диапазону цены)
            eff1 = vol1 / range1 if range1 > 0 else float('inf')
            eff2 = vol2 / range2 if range2 > 0 else float('inf')
            eff3 = vol3 / range3 if range3 > 0 else float('inf')
            
            # Условие поглощения: высокий объем с малым диапазоном, за которым следует больший диапазон
            if (eff1 > 2 * eff3 or eff2 > 2 * eff3) and range3 > 1.5 * max(range1, range2):
                # Определяем направление движения
                direction = 'buy' if df.iloc[i+2]['close'] > df.iloc[i+2]['open'] else 'sell'
                
                # Рассчитываем уверенность
                confidence = 0.7
                
                # Если объем третьей свечи тоже высокий, повышаем уверенность
                if vol3 > avg_vol * 1.2:
                    confidence += 0.1
                
                # Если движение цены очень значительное, повышаем уверенность
                if range3 > 2 * max(range1, range2):
                    confidence += 0.1
                
                # Создаем запись о паттерне
                pattern = {
                    'type': 'volume',
                    'subtype': 'Volume_Absorption',
                    'start_idx': i,
                    'end_idx': i+2,
                    'confidence': round(confidence, 2),
                    'expected_direction': direction,
                    'description': 'Обнаружено поглощение объема, указывающее на вероятное движение цены',
                    'volume_efficiency': [round(eff1, 2), round(eff2, 2), round(eff3, 2)]
                }
                
                patterns.append(pattern)
        
        return patterns


# ============================================================
# ИНТЕГРАТОР СИГНАЛОВ
# ============================================================

class SignalIntegrator:
    """
    Класс для интеграции сигналов от разных индикаторов и паттернов.
    
    Оценивает и объединяет сигналы от различных источников, учитывая их веса и таймфреймы,
    для формирования общей оценки и рекомендации по торговле.
    """
    
    def __init__(self, weights: Dict = None):
        """
        Инициализирует интегратор сигналов.
        
        Args:
            weights: Словарь с весами для разных индикаторов и паттернов
        """
        self.weights = weights or {
            # Веса индикаторов
            'RSI': 1.0,
            'MACD': 1.0,
            'EMA': 0.8,
            'Bollinger': 0.8,
            'ATR': 0.5,
            'ADX': 0.7,
            'Stochastic': 0.9,
            'OBV': 0.7,
            
            # Веса паттернов
            'price': 1.2,
            'volume': 1.1,
            'volatility': 0.9,
            'orderbook': 1.0,
            'imbalance': 0.8
        }
    
    def integrate_signals(self, signals: List[Dict], timeframe: str) -> Dict:
        """
        Интегрирует сигналы от разных источников.
        
        Args:
            signals: Список сигналов от разных индикаторов и паттернов
            timeframe: Таймфрейм, для которого интегрируются сигналы
            
        Returns:
            Словарь с результатами интеграции сигналов
        """
        # Базовая оценка для покупки и продажи
        buy_score = 0.0
        sell_score = 0.0
        
        # Количество сигналов для покупки и продажи
        buy_count = 0
        sell_count = 0
        
        # Список всех обработанных сигналов для включения в результат
        processed_signals = []
        
        # Обрабатываем все сигналы
        for signal in signals:
            # Получаем вес для этого типа индикатора или паттерна
            weight = 1.0  # Вес по умолчанию
            
            if 'indicator' in signal:
                weight = self.weights.get(signal['indicator'], 1.0)
            elif 'type' in signal:
                weight = self.weights.get(signal['type'], 1.0)
                
                # Для паттернов также учитываем подтип, если вес для него определен
                if 'subtype' in signal:
                    weight = self.weights.get(signal['subtype'], weight)
            
            # Корректируем вес в зависимости от таймфрейма
            # Больший вес для старших таймфреймов, но с обновленными коэффициентами
            tf_multiplier = 1.0
            if timeframe == '1h':
                tf_multiplier = 1.3
            elif timeframe == '4h':
                tf_multiplier = 1.3
            elif timeframe == '24h':
                tf_multiplier = 1.3
            elif timeframe == '7d':
                tf_multiplier = 1.1
            
            weight *= tf_multiplier
            
            # Определяем силу сигнала
            signal_strength = signal.get('strength', 0.5) * weight
            if 'confidence' in signal:
                signal_strength *= signal['confidence']
            
            # Копируем сигнал для включения в результат
            processed_signal = signal.copy()
            processed_signal['weighted_strength'] = round(signal_strength, 2)
            
            # Суммируем оценки в зависимости от типа сигнала
            if signal.get('signal_type') == 'buy' or signal.get('expected_direction') == 'buy':
                buy_score += signal_strength
                buy_count += 1
                processed_signal['contribution'] = 'buy'
            elif signal.get('signal_type') == 'sell' or signal.get('expected_direction') == 'sell':
                sell_score += signal_strength
                sell_count += 1
                processed_signal['contribution'] = 'sell'
            
            processed_signals.append(processed_signal)
        
        # Нормализация оценок
        buy_score = buy_score / buy_count if buy_count > 0 else 0
        sell_score = sell_score / sell_count if sell_count > 0 else 0
        
        # Определение финального сигнала
        final_signal = 'neutral'
        if buy_score > sell_score and buy_score > 0.6:
            final_signal = 'buy'
        elif sell_score > buy_score and sell_score > 0.6:
            final_signal = 'sell'
        
        # Формируем результат
        result = {
            'timeframe': timeframe,
            'buy_score': round(buy_score, 2),
            'sell_score': round(sell_score, 2),
            'buy_count': buy_count,
            'sell_count': sell_count,
            'signal': final_signal,
            'confidence': round(max(buy_score, sell_score), 2),
            'processed_signals': processed_signals
        }
        
        return result


# ============================================================
# ВРЕМЕННОЙ СТРАТИФИКАТОР
# ============================================================

class TimeframeStrategist:
    """
    Класс для определения стратегии использования таймфреймов.
    
    Оценивает потенциал монеты на основе анализа старших таймфреймов и определяет,
    нужен ли более детальный анализ на младших таймфреймах.
    """
    
    def __init__(self, config: Dict = None):
        """
        Инициализирует стратега по таймфреймам.
        
        Args:
            config: Словарь с конфигурацией стратега
        """
        self.config = config or {
            'initial_analysis': ['7d', '24h', '4h', '1h'],  # Таймфреймы для начального анализа
            'detailed_analysis': ['30m', '15m', '5m'],     # Таймфреймы для детального анализа
            'potential_threshold': 0.7,                    # Порог потенциала для детального анализа
            'profit_target': 0.05                         # Целевая прибыль (5%)
        }
    
    def evaluate_potential(self, symbol: str, signals: Dict) -> float:
        """
        Оценивает потенциал монеты на основе сигналов по различным таймфреймам.
        
        Args:
            symbol: Символ монеты
            signals: Словарь с сигналами по разным таймфреймам
            
        Returns:
            Оценка потенциала монеты (от 0 до 1)
        """
        potential = 0.0
        weight_sum = 0.0
        
        # Значимость таймфреймов (обновлено в соответствии с рекомендациями)
        timeframe_weights = {
            '7d': 0.1,
            '24h': 0.3,
            '4h': 0.3,
            '1h': 0.3
        }
        
        for tf, weight in timeframe_weights.items():
            if tf in signals:
                # Учитываем buy_score для потенциала роста
                potential += signals[tf].get('buy_score', 0) * weight
                weight_sum += weight
        
        # Нормализуем потенциал
        if weight_sum > 0:
            potential /= weight_sum
        
        return potential
    
    def needs_detailed_analysis(self, symbol: str, signals: Dict) -> bool:
        """
        Определяет, нужен ли детальный анализ на меньших таймфреймах.
        
        Args:
            symbol: Символ монеты
            signals: Словарь с сигналами по разным таймфреймам
            
        Returns:
            True, если нужен детальный анализ, иначе False
        """
        potential = self.evaluate_potential(symbol, signals)
        
        # Если потенциал превышает порог, рекомендуем детальный анализ
        return potential >= self.config['potential_threshold']
    
    def get_execution_plan(self, symbol: str, signals: Dict = None) -> Dict:
        """
        Создает план выполнения анализа для конкретной монеты.
        
        Args:
            symbol: Символ монеты
            signals: Словарь с сигналами по разным таймфреймам (если уже есть)
            
        Returns:
            Словарь с планом выполнения анализа
        """
        plan = {
            'symbol': symbol,
            'initial_analysis': self.config['initial_analysis'].copy(),
            'detailed_analysis': [],
            'potential': 0.0
        }
        
        # Если у нас уже есть сигналы, определяем необходимость детального анализа
        if signals:
            potential = self.evaluate_potential(symbol, signals)
            plan['potential'] = potential
            
            if potential >= self.config['potential_threshold']:
                plan['detailed_analysis'] = self.config['detailed_analysis'].copy()
        
        return plan


# ============================================================
# УПРАВЛЯЮЩИЙ КЛАСС INDICATOR ENGINE
# ============================================================

class IndicatorEngine:
    """
    Основной класс для управления анализом индикаторов.
    
    Координирует работу всех компонентов: загрузку данных, расчет индикаторов,
    обнаружение паттернов, интеграцию сигналов и формирование результатов.
    """
    
    def __init__(self, config: Dict):
        """
        Инициализирует движок индикаторов.
        
        Args:
            config: Словарь с конфигурацией движка
        """
        self.config = config
        
        # Инициализация утилит и хелперов
        self.data_cache = DataCache(
            use_redis=config.get('use_redis', False),
            redis_config=config.get('redis_config')
        )
        
        self.data_storage = DataStorage(
            base_path=config['paths']['BRAIN_DATA'],
            use_parquet=config.get('use_parquet', False)
        )
        
        self.parallel_processor = ParallelProcessor(
            max_workers=config['processing'].get('max_workers', 8),
            chunk_size=config['processing'].get('batch_size', 10)
        )
        
        # Инициализация индикаторов
        self.indicators = self._init_indicators()
        
        # Инициализация детекторов паттернов
        self.pattern_detectors = self._init_pattern_detectors()
        
        # Инициализация интегратора сигналов
        self.signal_integrator = SignalIntegrator(weights=config.get('indicator_weights'))
        
        # Инициализация стратега по таймфреймам
        self.timeframe_strategist = TimeframeStrategist(config.get('timeframe_strategy'))
        
        # Настройка логирования
        self.logger = logger
    
    def _init_indicators(self) -> Dict[str, BaseIndicator]:
        """
        Инициализирует индикаторы согласно конфигурации.
        
        Returns:
            Словарь с инициализированными индикаторами
        """
        indicators = {}
        
        # Добавляем RSI (из phase1, если включен)
        if self.config.get('indicators', {}).get('phase1', {}).get('momentum', {}).get('RSI', {}).get('enabled', True):
            rsi_params = self.config.get('indicators', {}).get('phase1', {}).get('momentum', {}).get('RSI', {})
            indicators['RSI'] = RSI(params=rsi_params)
        
        # Добавляем MACD (из phase1, если включен)
        if self.config.get('indicators', {}).get('phase1', {}).get('trend', {}).get('MACD', {}).get('enabled', True):
            macd_params = self.config.get('indicators', {}).get('phase1', {}).get('trend', {}).get('MACD', {})
            indicators['MACD'] = MACD(params=macd_params)
        
        # Добавляем другие индикаторы по мере реализации
        # ...
        
        return indicators
    
    def _init_pattern_detectors(self) -> Dict[str, PatternDetector]:
        """
        Инициализирует детекторы паттернов согласно конфигурации.
        
        Returns:
            Словарь с инициализированными детекторами паттернов
        """
        detectors = {}
        
        # Добавляем детектор ценовых паттернов, если включен
        if self.config.get('patterns', {}).get('price', {}).get('enabled', False):
            detectors['price'] = PricePatternDetector(params=self.config.get('patterns', {}).get('price', {}))
        
        # Добавляем детектор объемных паттернов, если включен
        if self.config.get('patterns', {}).get('volume', {}).get('enabled', False):
            detectors['volume'] = VolumePatternDetector(params=self.config.get('patterns', {}).get('volume', {}))
        
        # Добавляем другие детекторы по мере реализации
        # ...
        
        return detectors
    
    def load_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Загружает данные для указанного символа и таймфрейма.
        Поддерживает специализированный JSON формат проекта MONSTER.
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            DataFrame с данными или None, если данные не найдены
        """
        try:
            # Базовая директория с данными
            base_path = os.path.join(self.config['paths']['DATA_ENGINE']['current'], timeframe)
            
            # Попробуем разные варианты имен файлов
            json_paths = [
                os.path.join(base_path, f"{symbol}_{timeframe}.json"),  # Формат symbol_timeframe.json
                os.path.join(base_path, f"{symbol}.json"),              # Формат symbol.json
                os.path.join(self.config['paths']['DATA_ENGINE']['current'], f"{symbol}_{timeframe}.json"),
                os.path.join(self.config['paths']['DATA_ENGINE']['current'], f"{symbol}.json")
            ]
            
            # Для дебага - выводим все проверяемые пути
            self.logger.debug(f"Проверяем наличие данных для {symbol} ({timeframe}) в следующих путях:")
            for path in json_paths:
                self.logger.debug(f"  - {path}: {'существует' if os.path.exists(path) else 'не существует'}")
            
            # Ищем первый существующий файл
            json_path = None
            for path in json_paths:
                if os.path.exists(path):
                    json_path = path
                    break
            
            if not json_path:
                self.logger.warning(f"Файлы с данными не найдены для {symbol} ({timeframe})")
                return None
            
            # Загружаем JSON данные
            self.logger.info(f"Загружаем данные из JSON: {json_path}")
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            # Преобразуем специфический формат JSON в DataFrame
            rows = []
            for candle in data:
                # Проверяем наличие необходимых ключей
                if 'quote' not in candle or 'USD' not in candle['quote']:
                    continue
                    
                # Получаем данные свечи
                quote = candle['quote']['USD']
                
                # Создаем запись для DataFrame
                row = {
                    'timestamp': quote.get('timestamp', candle.get('time_close')),
                    'open': quote.get('open', 0.0),
                    'high': quote.get('high', 0.0),
                    'low': quote.get('low', 0.0),
                    'close': quote.get('close', 0.0),
                    'volume': quote.get('volume', 0.0)
                }
                
                rows.append(row)
            
            # Если нет данных, возвращаем None
            if not rows:
                self.logger.warning(f"В JSON файле не найдено данных в ожидаемом формате для {symbol} ({timeframe})")
                return None
            
            # Создаем DataFrame
            df = pd.DataFrame(rows)
            
            # Преобразуем timestamp в datetime и устанавливаем как индекс
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Сортируем по индексу (времени)
            df.sort_index(inplace=True)
            
            self.logger.info(f"Успешно загружены данные для {symbol} ({timeframe}), {len(df)} свечей")
            return df
            
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных для {symbol} ({timeframe}): {e}")
            self.logger.debug(traceback.format_exc())
            return None
    
    def analyze_timeframe(self, symbol: str, timeframe: str, indicator_list: List[str] = None) -> Dict:
        """
        Анализирует данные для указанного символа и таймфрейма.
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            indicator_list: Список индикаторов для расчета (если None, используются все доступные)
            
        Returns:
            Словарь с результатами анализа
        """
        start_time = time.time()
        
        # Загружаем данные
        df = self.load_data(symbol, timeframe)
        if df is None:
            return {'success': False, 'error': 'Не удалось загрузить данные'}
        
        # Если список индикаторов не указан, используем все доступные
        if indicator_list is None:
            indicator_list = list(self.indicators.keys())
        
        # Применяем индикаторы
        all_signals = []
        for ind_name in indicator_list:
            if ind_name in self.indicators:
                try:
                    # Рассчитываем индикатор
                    df = self.indicators[ind_name].calculate(df)
                    
                    # Генерируем сигналы
                    signals = self.indicators[ind_name].generate_signals(df)
                    all_signals.extend(signals)
                except Exception as e:
                    self.logger.error(f"Ошибка при расчете индикатора {ind_name} для {symbol} ({timeframe}): {e}")
                    self.logger.debug(traceback.format_exc())
        
        # Применяем детекторы паттернов
        for detector_name, detector in self.pattern_detectors.items():
            try:
                patterns = detector.detect(df)
                all_signals.extend(patterns)
            except Exception as e:
                self.logger.error(f"Ошибка при поиске паттернов {detector_name} для {symbol} ({timeframe}): {e}")
                self.logger.debug(traceback.format_exc())
        
        # Интегрируем сигналы
        integrated_signal = self.signal_integrator.integrate_signals(all_signals, timeframe)
        
        execution_time = time.time() - start_time
        self.logger.info(f"Анализ {symbol} ({timeframe}) завершен за {execution_time:.2f} сек")
        
        # Возвращаем результат
        return {
            'success': True,
            'symbol': symbol,
            'timeframe': timeframe,
            'signals': all_signals,
            'integrated_signal': integrated_signal,
            'execution_time': execution_time
        }
    
    def analyze_symbol(self, symbol: str, timeframes: List[str] = None) -> Dict:
        """
        Анализирует данные для указанного символа по всем таймфреймам.
        
        Args:
            symbol: Символ монеты
            timeframes: Список таймфреймов для анализа (если None, используются все доступные)
            
        Returns:
            Словарь с результатами анализа
        """
        start_time = time.time()
        
        # Если таймфреймы не указаны, используем все доступные
        if timeframes is None:
            timeframes = list(self.config.get('TIMEFRAMES', {}).keys())
        
        results = {}
        signals_by_tf = {}
        
        # Сначала анализируем основные таймфреймы
        initial_timeframes = self.timeframe_strategist.config['initial_analysis']
        available_initial_tfs = [tf for tf in initial_timeframes if tf in timeframes]
        
        for tf in available_initial_tfs:
            result = self.analyze_timeframe(symbol, tf)
            if result['success']:
                results[tf] = result
                signals_by_tf[tf] = result['integrated_signal']
        
        # Определяем, нужен ли детальный анализ
        needs_detail = self.timeframe_strategist.needs_detailed_analysis(symbol, signals_by_tf)
        
        # Если нужен детальный анализ, анализируем оставшиеся таймфреймы
        if needs_detail:
            detailed_timeframes = self.timeframe_strategist.config['detailed_analysis']
            available_detailed_tfs = [tf for tf in detailed_timeframes if tf in timeframes]
            
            for tf in available_detailed_tfs:
                result = self.analyze_timeframe(symbol, tf)
                if result['success']:
                    results[tf] = result
                    signals_by_tf[tf] = result['integrated_signal']
        
        # Определяем общий сигнал по всем таймфреймам
        overall_signal = self._determine_overall_signal(signals_by_tf)
        
        execution_time = time.time() - start_time
        self.logger.info(f"Анализ символа {symbol} завершен за {execution_time:.2f} сек")
        
        # Возвращаем результат
        return {
            'success': True,
            'symbol': symbol,
            'results': results,
            'signals_by_timeframe': signals_by_tf,
            'overall_signal': overall_signal,
            'execution_time': execution_time,
            'detailed_analysis_performed': needs_detail
        }
    
    def _determine_overall_signal(self, signals_by_tf: Dict) -> Dict:
        """
        Определяет общий сигнал на основе сигналов всех таймфреймов.
        
        Args:
            signals_by_tf: Словарь с сигналами по разным таймфреймам
            
        Returns:
            Словарь с общим сигналом
        """
        # Базовые оценки для покупки и продажи
        buy_score = 0.0
        sell_score = 0.0
        
        # Веса таймфреймов (обновлены в соответствии с рекомендациями)
        tf_weights = {
            '7d': 0.1,
            '24h': 0.3,
            '4h': 0.3,
            '1h': 0.3,
            '30m': 0.1,
            '15m': 0.1,
            '5m': 0.05
        }
        
        # Суммируем оценки с учетом весов таймфреймов
        weight_sum = 0.0
        for tf, signal in signals_by_tf.items():
            weight = tf_weights.get(tf, 0.1)
            buy_score += signal.get('buy_score', 0) * weight
            sell_score += signal.get('sell_score', 0) * weight
            weight_sum += weight
        
        # Нормализуем оценки
        if weight_sum > 0:
            buy_score /= weight_sum
            sell_score /= weight_sum
        
        # Определяем финальный сигнал
        signal = 'neutral'
        if buy_score > sell_score and buy_score > 0.6:
            signal = 'buy'
        elif sell_score > buy_score and sell_score > 0.6:
            signal = 'sell'
        
        # Рассчитываем уверенность
        confidence = max(buy_score, sell_score)
        
        return {
            'signal': signal,
            'buy_score': round(buy_score, 2),
            'sell_score': round(sell_score, 2),
            'confidence': round(confidence, 2)
        }
    
    def process_symbols(self, symbols: List[str], timeframes: List[str] = None) -> Dict:
        """
        Обрабатывает список символов параллельно.
        
        Args:
            symbols: Список символов для обработки
            timeframes: Список таймфреймов для анализа (если None, используются все доступные)
            
        Returns:
            Словарь с результатами обработки
        """
        def process_symbol_batch(symbol_batch):
            batch_results = {
                'successful_symbols': 0,
                'failed_symbols': 0,
                'total_signals': 0,
                'results': {}
            }
            
            for symbol in symbol_batch:
                try:
                    result = self.analyze_symbol(symbol, timeframes)
                    if result['success']:
                        batch_results['successful_symbols'] += 1
                        # Подсчитываем общее количество сигналов
                        signal_count = 0
                        for tf, tf_result in result.get('results', {}).items():
                            signal_count += len(tf_result.get('signals', []))
                        batch_results['total_signals'] += signal_count
                        batch_results['results'][symbol] = result
                    else:
                        batch_results['failed_symbols'] += 1
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке символа {symbol}: {e}")
                    self.logger.debug(traceback.format_exc())
                    batch_results['failed_symbols'] += 1
            
            return batch_results
        
        # Обрабатываем символы параллельно
        results = self.parallel_processor.process_symbols(symbols, process_symbol_batch)
        
        # Определяем топ символы (с наибольшим потенциалом)
        top_symbols = self._get_top_symbols(results['results'], limit=20)
        results['top_symbols'] = top_symbols
        
        return results
    
    def _get_top_symbols(self, results: Dict, limit: int = 20) -> List[str]:
        """
        Возвращает список топ символов с наибольшим потенциалом.
        
        Args:
            results: Словарь с результатами обработки символов
            limit: Максимальное количество символов в списке
            
        Returns:
            Список символов с наибольшим потенциалом
        """
        symbol_scores = []
        
        for symbol, result in results.items():
            # Получаем общий сигнал
            overall_signal = result.get('overall_signal', {})
            
            # Если сигнал на покупку и уверенность высокая
            if overall_signal.get('signal') == 'buy' and overall_signal.get('confidence', 0) > 0.6:
                symbol_scores.append((symbol, overall_signal.get('buy_score', 0)))
        
        # Сортируем по убыванию оценки
        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        
        # Возвращаем только символы (без оценок)
        return [symbol for symbol, score in symbol_scores[:limit]]
    
    def run(self, symbols: List[str] = None, timeframes: List[str] = None) -> Dict:
        """
        Запускает основной процесс анализа.
        
        Args:
            symbols: Список символов для анализа
            timeframes: Список таймфреймов для анализа
            
        Returns:
            Словарь с результатами анализа
        """
        # Если символы не указаны, загружаем их из данных
        if symbols is None:
            symbols = self._load_symbols()
        
        # Если таймфреймы не указаны, используем все доступные
        if timeframes is None:
            timeframes = [tf for tf, config in self.config.get('TIMEFRAMES', {}).items() if config.get('enabled', True)]
        
        # Запускаем обработку
        start_time = time.time()
        results = self.process_symbols(symbols, timeframes)
        execution_time = time.time() - start_time
        
        # Добавляем время выполнения
        results['execution_time'] = execution_time
        
        # Логируем результаты
        self.logger.info(f"Обработка индикаторов завершена за {execution_time:.2f} сек. "
                         f"Успешно: {results['successful_symbols']}/{results['total_symbols']} монет")
        
        # Отправляем уведомление в Telegram, если включено
        if self.config.get('TELEGRAM_NOTIFICATIONS', {}).get('enabled', True):
            self._send_telegram_notification(results)
        
        return results
    
    def _load_symbols(self) -> List[str]:
        """
        Загружает список символов для анализа из директории данных.
        
        Returns:
            Список символов для анализа
        """
        symbols = []
        
        try:
            # Определяем директорию с данными для основного таймфрейма (например, 1h)
            main_timeframe = '1h'  # Можно настроить через конфигурацию
            data_dir = os.path.join(self.config['paths']['DATA_ENGINE']['current'], main_timeframe)
            
            # Проверяем наличие директории
            if not os.path.exists(data_dir):
                self.logger.warning(f"Директория с данными не найдена: {data_dir}")
                # Возвращаем список для тестирования
                return ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOT', 'DOGE', 'AVAX', 'LINK']
            
            # Собираем список символов из файлов
            for filename in os.listdir(data_dir):
                if filename.endswith(f"_{main_timeframe}.csv"):
                    symbol = filename.split('_')[0]
                    symbols.append(symbol)
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке списка символов: {e}")
            # Возвращаем список для тестирования
            return ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOT', 'DOGE', 'AVAX', 'LINK']
        
        # Если список пуст, возвращаем список для тестирования
        if not symbols:
            self.logger.warning("Список символов пуст. Используем тестовый список.")
            return ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOT', 'DOGE', 'AVAX', 'LINK']
        
        return symbols
    
    def _send_telegram_notification(self, results: Dict) -> None:
        """
        Отправляет уведомление о результатах анализа в Telegram.
        
        Args:
            results: Словарь с результатами анализа
        """
        try:
            # Формируем сообщение
            message = self._format_telegram_message(results)
            
            # Отправляем сообщение, используя правильную функцию
            send_telegram(message, priority="normal")
            
            self.logger.info("Уведомление в Telegram отправлено успешно")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке уведомления в Telegram: {e}")
    
    def _format_telegram_message(self, results: Dict) -> str:
        """
        Форматирует сообщение для отправки в Telegram.
        
        Args:
            results: Словарь с результатами анализа
            
        Returns:
            Отформатированное сообщение
        """
        # Эмодзи для улучшения визуального восприятия
        emoji = {
            'success': '✅',
            'info': 'ℹ️',
            'time': '⏱️',
            'count': '📊',
            'top': '🔝',
            'buy': '📈',
            'sell': '📉',
            'neutral': '➖',
            'signal': '🔔'
        }
        
        # Формируем сообщение
        message_parts = [
            f"{emoji['info']} <b>Обработка индикаторов завершена {'успешно' if results.get('success', False) else 'с ошибками'}</b>",
            f"{emoji['time']} Время выполнения: {results.get('execution_time', 0):.2f} сек",
            f"{emoji['count']} Обработано: {results.get('successful_symbols', 0)}/{results.get('total_symbols', 0)} монет"
        ]
        
        # Добавляем информацию о сигналах
        total_signals = results.get('total_signals', 0)
        if total_signals > 0:
            message_parts.append(f"{emoji['signal']} Всего сигналов: {total_signals}")
        
        # Добавляем топ символы
        top_symbols = results.get('top_symbols', [])
        if top_symbols:
            # Ограничиваем количество символов в сообщении
            display_symbols = top_symbols[:10]
            message_parts.append(f"{emoji['top']} Топ символы: {', '.join(display_symbols)}")
        
        # Детальная информация о топ символах (если есть)
        if top_symbols and results.get('results'):
            message_parts.append("\n<b>Детальная информация о топ символах:</b>")
            
            for symbol in top_symbols[:5]:  # Показываем только первые 5 символов
                if symbol in results.get('results', {}):
                    symbol_result = results['results'][symbol]
                    overall_signal = symbol_result.get('overall_signal', {})
                    signal_type = overall_signal.get('signal', 'neutral')
                    confidence = overall_signal.get('confidence', 0)
                    
                    # Эмодзи в зависимости от типа сигнала
                    signal_emoji = emoji.get(signal_type, emoji['neutral'])
                    
                    message_parts.append(
                        f"{signal_emoji} <b>{symbol}</b>: "
                        f"Сигнал: {signal_type}, "
                        f"Уверенность: {confidence:.2f}"
                    )
                    
                    # Добавляем информацию о паттернах (не более 2-3 для каждого символа)
                    patterns = []
                    for tf, tf_result in symbol_result.get('results', {}).items():
                        for signal in tf_result.get('signals', []):
                            if 'type' in signal and signal.get('type') == 'price':
                                patterns.append(
                                    f"{tf} - {signal.get('subtype', 'Неизвестный паттерн')} "
                                    f"({signal.get('expected_direction', 'neutral')})"
                                )
                    
                    if patterns:
                        message_parts.append(f"   Паттерны: {', '.join(patterns[:3])}")
        
        # Соединяем все части сообщения
        return "\n".join(message_parts)


def test_indicator_engine():
    """
    Запускает тестовый анализ индикаторов на реальных данных.
    """
    logger.info("Начинаем тестовый анализ индикаторов...")
    
    # Загружаем конфигурацию для IndicatorEngine
    # Используем INDICATOR_ENGINE из импортированного config.py
    config = {
        'paths': PATHS,
        'TIMEFRAMES': TIMEFRAMES,
        'processing': PROCESSING,
        'indicators': INDICATORS.get('phase1', {}),  # Берем индикаторы первой фазы
        'patterns': PATTERNS,
        'timeframe_strategy': {
            'initial_analysis': ['7d', '24h', '4h', '1h'],
            'detailed_analysis': ['30m', '15m', '5m'],
            'potential_threshold': 0.7,
            'profit_target': 0.05
        },
        'TELEGRAM_NOTIFICATIONS': {
            'enabled': True
        }
    }
    
    # Проверим пути к данным
    logger.info(f"Пути к данным: {PATHS['DATA_ENGINE']['current']}")
    
    # Создаем экземпляр IndicatorEngine
    logger.info("Создаем экземпляр IndicatorEngine...")
    engine = IndicatorEngine(config)
    
    # Выбираем тестовые монеты (сокращаем список для быстрого теста)
    test_symbols = ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOT', 'DOGE', 'AVAX', 'LINK']
    logger.info(f"Тестовые монеты: {', '.join(test_symbols)}")
    
    # Ограничиваем таймфреймы для быстрого тестирования
    test_timeframes = ['1h']
    logger.info(f"Тестовые таймфреймы: {', '.join(test_timeframes)}")
    
    # Запускаем анализ
    logger.info("Запуск тестового анализа...")
    results = engine.run(test_symbols, test_timeframes)
    
    # Выводим результаты
    logger.info(f"Анализ завершен. Успешно обработано {results.get('successful_symbols', 0)} монет.")
    if 'top_symbols' in results:
        logger.info(f"Топ символы: {', '.join(results['top_symbols'])}")
    
    return results

# ОЧЕНЬ ВАЖНО: убедитесь, что этот блок присутствует в конце файла
if __name__ == "__main__":
    print("Запуск тестового анализа индикаторов...")
    test_results = test_indicator_engine()
    print("Тестовый анализ завершен")
