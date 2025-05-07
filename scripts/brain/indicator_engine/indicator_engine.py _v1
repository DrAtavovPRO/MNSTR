#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
IndicatorEngine 2.0 для системы МОНСТР (Масштабируемая Оптимизированная 
Нейросетевая Система Торговых Решений)

Модуль для расчета технических индикаторов, паттернов, дивергенций и анализа
ордербуков с многотаймфреймным подходом и оптимизированной обработкой.

Основные особенности:
- Все типы индикаторов (трендовые, моментум, волатильность, объемные)
- Обнаружение ценовых, объемных паттернов и паттернов волатильности
- Детектирование дивергенций и подтверждений между таймфреймами
- Интеграция с данными ордербуков для анализа рыночной микроструктуры
- Оптимизированная параллельная и асинхронная обработка
- Весовой многотаймфреймный анализ с фильтрацией монет
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
import asyncio
from functools import wraps, lru_cache
from enum import Enum
from dataclasses import dataclass
import warnings

# Управление путями
ROOT_DIR = '/home/monster_trading_v2'
sys.path.insert(0, ROOT_DIR)

from scripts.core.brain.data_engine import ThreadManager  # Переиспользуем ThreadManager из data_engine

# Проверка наличия pandas и numpy
try:
    import pandas as pd
    import numpy as np
    from scipy import stats
    import matplotlib.pyplot as plt
    from statsmodels.tsa.stattools import adfuller
    
    PANDAS_AVAILABLE = True
    SCIPY_AVAILABLE = True
    PLOTTING_AVAILABLE = True
except ImportError as e:
    if 'pandas' in str(e) or 'numpy' in str(e):
        PANDAS_AVAILABLE = False
    else:
        PANDAS_AVAILABLE = True
        
    if 'scipy' in str(e):
        SCIPY_AVAILABLE = False
    else:
        SCIPY_AVAILABLE = True
        
    if 'matplotlib' in str(e):
        PLOTTING_AVAILABLE = False
    else:
        PLOTTING_AVAILABLE = True
    
    print(f"Предупреждение: не удалось импортировать некоторые библиотеки: {e}")
    print("Будет использована базовая функциональность без оптимизаций")

# Импорт компонентов системы
try:
    from scripts.utils.brain_logger import get_logger, log_execution_time, send_telegram
    from config.brain.config import PATHS, TIMEFRAMES, API, PROCESSING, INDICATORS
    from scripts.core.brain.data_engine import ThreadManager  # Переиспользуем ThreadManager из data_engine
    LOGGER_IMPORTED = True
except ImportError as e:
    LOGGER_IMPORTED = False
    import logging
    import sys
    
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

# Настройка логгера
logger = get_logger('indicator_engine', 'indicators')

# ===========================================================================
# БЛОК 2: ОБЩИЕ УТИЛИТЫ
# ===========================================================================

def record_execution_time(operation_name: str, timeframe: Optional[str], execution_time: float) -> None:
    """
    Запись времени выполнения операций в лог без буферизации
    
    Args:
        operation_name: Название операции
        timeframe: Таймфрейм (опционально)
        execution_time: Время выполнения в секундах
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cpu_percent = min(int(execution_time/os.cpu_count()*100), 99)
        
        # Формируем запись
        if timeframe:
            timing_entry = f"[{timestamp}] {operation_name}: {execution_time:.2f} сек, CPU: {cpu_percent}% (TF: {timeframe})"
        else:
            timing_entry = f"[{timestamp}] {operation_name}: {execution_time:.2f} сек, CPU: {cpu_percent}%"
        
        # Прямая запись в файл
        timing_dir = os.path.join(ROOT_DIR, 'logs', 'timing')
        os.makedirs(timing_dir, exist_ok=True)
        
        timing_file = os.path.join(timing_dir, "time_indicators.log")
        
        with open(timing_file, 'a') as f:
            f.write(f"{timing_entry}\n")
                
    except Exception as e:
        logger.error(f"Ошибка записи времени выполнения: {e}")

def async_wrap(func):
    """
    Обертка для превращения синхронной функции в асинхронную
    
    Args:
        func: Синхронная функция
    
    Returns:
        Асинхронная версия функции
    """
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run

def profile(func):
    """
    Декоратор для профилирования функций
    
    Args:
        func: Функция для профилирования
    
    Returns:
        Обернутая функция с профилированием
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        # Получаем имя функции и класса, если доступно
        if hasattr(args[0].__class__, '__name__') and args[0].__class__.__name__ != 'type':
            class_name = args[0].__class__.__name__
            logger.debug(f"Профилирование: {class_name}.{func.__name__} выполнена за {end_time - start_time:.4f} сек")
        else:
            logger.debug(f"Профилирование: {func.__name__} выполнена за {end_time - start_time:.4f} сек")
        
        return result
    return wrapper

@lru_cache(maxsize=128)
def get_market_phase(df: pd.DataFrame, days_back: int = 30) -> str:
    """
    Определяет текущую фазу рынка на основе данных
    
    Args:
        df: DataFrame с OHLCV данными
        days_back: Количество дней для анализа
    
    Returns:
        str: Фаза рынка (trending_up, trending_down, ranging)
    """
    if df.empty or len(df) < days_back:
        return "uncertain"
    
    # Берем только последние N дней
    recent_df = df.iloc[-days_back:]
    
    # Проверяем наличие тренда с помощью метода ADFuller
    if SCIPY_AVAILABLE:
        try:
            # Используем тест Дики-Фуллера для проверки стационарности
            result = adfuller(recent_df['close'])
            p_value = result[1]
            
            # Проверка на наличие тренда (не стационарный временной ряд)
            if p_value > 0.05:  # p-value > 0.05 означает, что есть тренд
                # Определение направления тренда
                first_half = recent_df.iloc[:len(recent_df)//2]['close'].mean()
                second_half = recent_df.iloc[len(recent_df)//2:]['close'].mean()
                
                if second_half > first_half * 1.03:  # Рост более 3%
                    return "trending_up"
                elif first_half > second_half * 1.03:  # Падение более 3%
                    return "trending_down"
                else:
                    return "weak_trend"
            else:
                return "ranging"
        except:
            pass
    
    # Упрощенный анализ, если scipy недоступна
    first_price = recent_df['close'].iloc[0]
    last_price = recent_df['close'].iloc[-1]
    
    # Расчет процентного изменения
    change_percent = (last_price - first_price) / first_price * 100
    
    # Определение типа рынка по проценту изменения
    if change_percent > 10:
        return "trending_up"
    elif change_percent < -10:
        return "trending_down"
    else:
        # Проверка на боковое движение через волатильность
        avg_tr = calculate_atr_simple(recent_df, 14).mean()
        avg_price = recent_df['close'].mean()
        volatility = avg_tr / avg_price * 100
        
        if volatility < 2:  # Низкая волатильность
            return "ranging_tight"
        else:
            return "ranging_volatile"

def calculate_atr_simple(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Простой расчет ATR без использования сторонних библиотек
    
    Args:
        df: DataFrame с OHLCV данными
        period: Период ATR
    
    Returns:
        pd.Series: ATR
    """
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    
    return true_range.rolling(period).mean()

def is_consolidation(df: pd.DataFrame, window: int = 20, threshold: float = 0.03) -> bool:
    """
    Определяет, находится ли рынок в состоянии консолидации
    
    Args:
        df: DataFrame с OHLCV данными
        window: Размер окна для анализа
        threshold: Порог для определения консолидации
    
    Returns:
        bool: True, если рынок в состоянии консолидации
    """
    if len(df) < window:
        return False
    
    recent = df.iloc[-window:]
    max_price = recent['high'].max()
    min_price = recent['low'].min()
    
    # Рассчитываем процентное отклонение
    deviation = (max_price - min_price) / min_price
    
    return deviation < threshold

def get_market_strength(df: pd.DataFrame, lookback: int = 20) -> float:
    """
    Расчет силы рынка на основе комбинации индикаторов
    
    Args:
        df: DataFrame с OHLCV данными
        lookback: Количество баров для анализа
    
    Returns:
        float: Индекс силы рынка от -1 (сильно медвежий) до 1 (сильно бычий)
    """
    if df.empty or len(df) < lookback:
        return 0.0
    
    recent = df.iloc[-lookback:]
    
    # Направление цены
    price_direction = 1 if recent['close'].iloc[-1] > recent['close'].iloc[0] else -1
    
    # Объемный анализ (рост объема подтверждает силу движения)
    volume_trend = 1 if recent['volume'].iloc[-1] > recent['volume'].iloc[0] else -1
    
    # Импульс (скорость изменения цены)
    momentum = (recent['close'].iloc[-1] - recent['close'].iloc[0]) / recent['close'].iloc[0]
    momentum_signal = np.clip(momentum * 10, -1, 1)  # Нормализация и ограничение
    
    # Общий индекс силы рынка
    strength = (price_direction * 0.4) + (volume_trend * 0.2) + (momentum_signal * 0.4)
    
    return np.clip(strength, -1, 1)

# ===========================================================================
# БЛОК 3: АБСТРАКТНЫЕ КЛАССЫ И ИНТЕРФЕЙСЫ
# ===========================================================================

class IndicatorType(Enum):
    """Типы индикаторов"""
    TREND = 'trend'          # Трендовые индикаторы (MACD, EMA, ADX и т.д.)
    MOMENTUM = 'momentum'    # Индикаторы моментума (RSI, Stochastic и т.д.)
    VOLATILITY = 'volatility'  # Индикаторы волатильности (ATR, Bollinger и т.д.)
    VOLUME = 'volume'        # Объемные индикаторы (OBV, Delta, VSA и т.д.)
    PATTERN = 'pattern'      # Паттерны (ценовые, объемные, волатильности)
    CUSTOM = 'custom'        # Пользовательские индикаторы

class SignalType(Enum):
    """Типы сигналов"""
    STRONG_BUY = 'strong_buy'  # Сильный сигнал на покупку
    BUY = 'buy'               # Сигнал на покупку
    WEAK_BUY = 'weak_buy'     # Слабый сигнал на покупку
    NEUTRAL = 'neutral'       # Нейтральный сигнал
    WEAK_SELL = 'weak_sell'   # Слабый сигнал на продажу
    SELL = 'sell'             # Сигнал на продажу
    STRONG_SELL = 'strong_sell'  # Сильный сигнал на продажу
    UNDEFINED = 'undefined'   # Неопределенный сигнал

class PatternType(Enum):
    """Типы паттернов"""
    PRICE = 'price'              # Ценовые паттерны
    VOLUME = 'volume'            # Объемные паттерны
    VOLATILITY = 'volatility'    # Паттерны волатильности
    COMBINED = 'combined'        # Комбинированные паттерны
    ORDERBOOK = 'orderbook'      # Паттерны ордербука

class DivergenceType(Enum):
    """Типы дивергенций"""
    REGULAR_BULLISH = 'regular_bullish'  # Регулярная бычья дивергенция
    REGULAR_BEARISH = 'regular_bearish'  # Регулярная медвежья дивергенция
    HIDDEN_BULLISH = 'hidden_bullish'    # Скрытая бычья дивергенция
    HIDDEN_BEARISH = 'hidden_bearish'    # Скрытая медвежья дивергенция

@dataclass
class Signal:
    """Класс для хранения информации о сигнале"""
    type: SignalType                 # Тип сигнала
    strength: float                  # Сила сигнала от 0 до 1
    indicator: str                   # Индикатор, который сгенерировал сигнал
    timestamp: datetime              # Время сигнала
    timeframe: str                   # Таймфрейм
    price: float                     # Цена в момент сигнала
    description: str = ''            # Описание сигнала
    stop_loss: Optional[float] = None  # Уровень стоп-лосса
    take_profit: Optional[float] = None  # Уровень тейк-профита
    confirmation: List[str] = None   # Списое подтверждений сигнала
    
    def __post_init__(self):
        if self.confirmation is None:
            self.confirmation = []

@dataclass
class Pattern:
    """Класс для хранения информации о паттерне"""
    type: PatternType                # Тип паттерна
    subtype: str                     # Подтип (имя паттерна)
    start_idx: int                   # Индекс начала паттерна
    end_idx: int                     # Индекс конца паттерна
    confidence: float                # Уверенность в паттерне от 0 до 1
    expected_direction: SignalType   # Ожидаемое направление после паттерна
    description: str = ''            # Описание паттерна
    support_levels: List[float] = None  # Уровни поддержки
    resistance_levels: List[float] = None  # Уровни сопротивления
    price_targets: List[float] = None  # Целевые уровни цены
    
    def __post_init__(self):
        if self.support_levels is None:
            self.support_levels = []
        if self.resistance_levels is None:
            self.resistance_levels = []
        if self.price_targets is None:
            self.price_targets = []

@dataclass
class Divergence:
    """Класс для хранения информации о дивергенции"""
    type: DivergenceType             # Тип дивергенции
    price_point1: int                # Индекс первой точки цены
    price_point2: int                # Индекс второй точки цены
    indicator_point1: int            # Индекс первой точки индикатора
    indicator_point2: int            # Индекс второй точки индикатора
    indicator_name: str              # Название индикатора
    strength: float                  # Сила дивергенции от 0 до 1
    confirmed: bool = False          # Подтверждена ли дивергенция
    description: str = ''            # Описание дивергенции

class BaseIndicator(ABC):
    """
    Базовый абстрактный класс для всех индикаторов
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора
        
        Args:
            params: Параметры индикатора
        """
        self.params = params or {}
        self.name = self.__class__.__name__
        self.type = IndicatorType.CUSTOM  # По умолчанию пользовательский тип
        self.result = None  # Результат расчета индикатора
        self.signals = []  # Сигналы индикатора
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> Dict[str, Union[pd.Series, pd.DataFrame]]:
        """
        Расчет индикатора
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета
        """
        pass
    
    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе индикатора
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        pass
    
    @property
    @abstractmethod
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета индикатора
        
        Returns:
            List[str]: Список колонок
        """
        pass
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Проверка данных на наличие необходимых колонок
        
        Args:
            df: DataFrame с данными
            
        Returns:
            bool: True, если данные валидны
        """
        if df is None or df.empty:
            logger.warning(f"{self.name}: Пустой DataFrame")
            return False
        
        for col in self.required_columns:
            if col not in df.columns:
                logger.warning(f"{self.name}: Колонка {col} отсутствует в данных")
                return False
        
        return True
    
    def get_weights(self, timeframe: str, market_phase: str) -> float:
        """
        Получение весов индикатора в зависимости от таймфрейма и фазы рынка
        
        Args:
            timeframe: Таймфрейм
            market_phase: Фаза рынка
            
        Returns:
            float: Вес индикатора
        """
        # Базовые веса для таймфрейма
        timeframe_weights = {
            '7d': 0.1,    # 10%
            '24h': 0.1,   # 10%
            '4h': 0.25,   # 25%
            '1h': 0.25,   # 25%
            '30m': 0.1,   # 10%
            '15m': 0.1,   # 10%
            '5m': 0.1     # 10%
        }
        
        # Корректировка весов в зависимости от фазы рынка и типа индикатора
        phase_multipliers = {
            # Трендовые индикаторы сильнее в трендовом рынке
            IndicatorType.TREND: {
                'trending_up': 1.2,
                'trending_down': 1.2,
                'weak_trend': 1.1,
                'ranging': 0.8,
                'ranging_tight': 0.7,
                'ranging_volatile': 0.8,
                'uncertain': 1.0
            },
            # Осцилляторы сильнее в боковом рынке
            IndicatorType.MOMENTUM: {
                'trending_up': 0.8,
                'trending_down': 0.8,
                'weak_trend': 0.9,
                'ranging': 1.2,
                'ranging_tight': 1.3,
                'ranging_volatile': 1.1,
                'uncertain': 1.0
            },
            # Индикаторы волатильности важны при смене фаз
            IndicatorType.VOLATILITY: {
                'trending_up': 0.9,
                'trending_down': 0.9,
                'weak_trend': 1.1,
                'ranging': 1.0,
                'ranging_tight': 1.2,
                'ranging_volatile': 1.3,
                'uncertain': 1.0
            },
            # Объемные индикаторы универсальны
            IndicatorType.VOLUME: {
                'trending_up': 1.1,
                'trending_down': 1.1,
                'weak_trend': 1.0,
                'ranging': 1.0,
                'ranging_tight': 1.0,
                'ranging_volatile': 1.1,
                'uncertain': 1.0
            },
            # Паттерны важны при смене фаз
            IndicatorType.PATTERN: {
                'trending_up': 1.0,
                'trending_down': 1.0,
                'weak_trend': 1.2,
                'ranging': 1.1,
                'ranging_tight': 1.2,
                'ranging_volatile': 1.1,
                'uncertain': 1.0
            },
            # Пользовательские индикаторы
            IndicatorType.CUSTOM: {
                'trending_up': 1.0,
                'trending_down': 1.0,
                'weak_trend': 1.0,
                'ranging': 1.0,
                'ranging_tight': 1.0,
                'ranging_volatile': 1.0,
                'uncertain': 1.0
            }
        }
        
        # Получаем базовый вес для таймфрейма
        weight = timeframe_weights.get(timeframe, 0.1)
        
        # Корректируем вес с учетом фазы рынка
        multiplier = phase_multipliers.get(self.type, {}).get(market_phase, 1.0)
        
        return weight * multiplier

class BasePatternDetector(ABC):
    """
    Базовый абстрактный класс для детекторов паттернов
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора паттернов
        
        Args:
            params: Параметры детектора
        """
        self.params = params or {}
        self.name = self.__class__.__name__
        self.pattern_type = PatternType.PRICE  # По умолчанию ценовой паттерн
        self.patterns = []  # Обнаруженные паттерны
    
    @abstractmethod
    def detect(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение паттернов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        pass
    
    @property
    @abstractmethod
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для обнаружения паттернов
        
        Returns:
            List[str]: Список колонок
        """
        pass
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Проверка данных на наличие необходимых колонок
        
        Args:
            df: DataFrame с данными
            
        Returns:
            bool: True, если данные валидны
        """
        if df is None or df.empty:
            logger.warning(f"{self.name}: Пустой DataFrame")
            return False
        
        for col in self.required_columns:
            if col not in df.columns:
                logger.warning(f"{self.name}: Колонка {col} отсутствует в данных")
                return False
        
        return True

class BaseDivergenceDetector(ABC):
    """
    Базовый абстрактный класс для детекторов дивергенций
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора дивергенций
        
        Args:
            params: Параметры детектора
        """
        self.params = params or {}
        self.name = self.__class__.__name__
        self.divergences = []  # Обнаруженные дивергенции
    
    @abstractmethod
    def detect(self, df: pd.DataFrame, indicator_data: Dict[str, pd.Series]) -> List[Divergence]:
        """
        Обнаружение дивергенций
        
        Args:
            df: DataFrame с данными
            indicator_data: Словарь с данными индикаторов
            
        Returns:
            List[Divergence]: Список обнаруженных дивергенций
        """
        pass
    
    @property
    @abstractmethod
    def required_indicators(self) -> List[str]:
        """
        Список индикаторов, необходимых для обнаружения дивергенций
        
        Returns:
            List[str]: Список индикаторов
        """
        pass
    
    def validate_data(self, df: pd.DataFrame, indicator_data: Dict[str, pd.Series]) -> bool:
        """
        Проверка данных на наличие необходимых индикаторов
        
        Args:
            df: DataFrame с данными
            indicator_data: Словарь с данными индикаторов
            
        Returns:
            bool: True, если данные валидны
        """
        if df is None or df.empty:
            logger.warning(f"{self.name}: Пустой DataFrame")
            return False
        
        if indicator_data is None:
            logger.warning(f"{self.name}: Отсутствуют данные индикаторов")
            return False
        
        for indicator in self.required_indicators:
            if indicator not in indicator_data:
                logger.warning(f"{self.name}: Индикатор {indicator} отсутствует в данных")
                return False
        
        return True

class OrderbookAnalyzer(ABC):
    """
    Базовый абстрактный класс для анализаторов ордербуков
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация анализатора ордербуков
        
        Args:
            params: Параметры анализатора
        """
        self.params = params or {}
        self.name = self.__class__.__name__
        self.results = {}  # Результаты анализа
    
    @abstractmethod
    def analyze(self, orderbook_data: Dict) -> Dict:
        """
        Анализ данных ордербука
        
        Args:
            orderbook_data: Данные ордербука
            
        Returns:
            Dict: Результаты анализа
        """
        pass
    
    @abstractmethod
    def generate_signals(self, orderbook_data: Dict, price_data: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе анализа ордербука
        
        Args:
            orderbook_data: Данные ордербука
            price_data: Данные о ценах
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        pass
    
    def validate_data(self, orderbook_data: Dict) -> bool:
        """
        Проверка данных ордербука на валидность
        
        Args:
            orderbook_data: Данные ордербука
            
        Returns:
            bool: True, если данные валидны
        """
        if orderbook_data is None:
            logger.warning(f"{self.name}: Отсутствуют данные ордербука")
            return False
        
        if 'bids' not in orderbook_data or 'asks' not in orderbook_data:
            logger.warning(f"{self.name}: Отсутствуют bids или asks в данных ордербука")
            return False
        
        return True

class IndicatorFactory:
    """
    Фабрика для создания индикаторов
    """
    
    @staticmethod
    def create(indicator_type: str, indicator_name: str, params: Dict = None) -> Optional[BaseIndicator]:
        """
        Создание индикатора
        
        Args:
            indicator_type: Тип индикатора (trend, momentum, volatility, volume)
            indicator_name: Имя индикатора (MACD, RSI, ATR, OBV и т.д.)
            params: Параметры индикатора
            
        Returns:
            BaseIndicator: Созданный индикатор или None при ошибке
        """
        try:
            # Подготовка параметров
            params = params or {}
            
            # Создание индикатора в зависимости от типа и имени
            if indicator_type == 'trend':
                if indicator_name == 'MACD':
                    from IndicatorEngine.indicators.trend import MACD
                    return MACD(params)
                elif indicator_name == 'EMA':
                    from IndicatorEngine.indicators.trend import EMA
                    return EMA(params)
                elif indicator_name == 'ADX':
                    from IndicatorEngine.indicators.trend import ADX
                    return ADX(params)
                
            elif indicator_type == 'momentum':
                if indicator_name == 'RSI':
                    from IndicatorEngine.indicators.momentum import RSI
                    return RSI(params)
                elif indicator_name == 'Stochastic':
                    from IndicatorEngine.indicators.momentum import Stochastic
                    return Stochastic(params)
                
            elif indicator_type == 'volatility':
                if indicator_name == 'ATR':
                    from IndicatorEngine.indicators.volatility import ATR
                    return ATR(params)
                elif indicator_name == 'Bollinger':
                    from IndicatorEngine.indicators.volatility import BollingerBands
                    return BollingerBands(params)
                
            elif indicator_type == 'volume':
                if indicator_name == 'OBV':
                    from IndicatorEngine.indicators.volume import OBV
                    return OBV(params)
                elif indicator_name == 'Delta':
                    from IndicatorEngine.indicators.volume import DeltaVolume
                    return DeltaVolume(params)
                elif indicator_name == 'VSA':
                    from IndicatorEngine.indicators.volume import VSA
                    return VSA(params)
            
            # Если внешний импорт не сработал, используем встроенные классы
            # Трендовые индикаторы
            if indicator_name == 'MACD':
                return MACDIndicator(params)
            elif indicator_name == 'EMA':
                return EMAIndicator(params)
            elif indicator_name == 'ADX':
                return ADXIndicator(params)
            
            # Индикаторы моментума
            elif indicator_name == 'RSI':
                return RSIIndicator(params)
            elif indicator_name == 'Stochastic':
                return StochasticIndicator(params)
            
            # Индикаторы волатильности
            elif indicator_name == 'ATR':
                return ATRIndicator(params)
            elif indicator_name == 'Bollinger':
                return BollingerBandsIndicator(params)
            
            # Объемные индикаторы
            elif indicator_name == 'OBV':
                return OBVIndicator(params)
            elif indicator_name == 'Delta':
                return DeltaVolumeIndicator(params)
            elif indicator_name == 'VSA':
                return VSAIndicator(params)
            
            logger.warning(f"Индикатор {indicator_name} типа {indicator_type} не найден")
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании индикатора {indicator_name}: {e}")
            return None

class PatternFactory:
    """
    Фабрика для создания детекторов паттернов
    """
    
    @staticmethod
    def create(pattern_type: PatternType, params: Dict = None) -> Optional[BasePatternDetector]:
        """
        Создание детектора паттернов
        
        Args:
            pattern_type: Тип паттерна
            params: Параметры детектора
            
        Returns:
            BasePatternDetector: Созданный детектор паттернов или None при ошибке
        """
        try:
            # Подготовка параметров
            params = params or {}
            
            # Создание детектора в зависимости от типа
            if pattern_type == PatternType.PRICE:
                return PricePatternDetector(params)
            elif pattern_type == PatternType.VOLUME:
                return VolumePatternDetector(params)
            elif pattern_type == PatternType.VOLATILITY:
                return VolatilityPatternDetector(params)
            elif pattern_type == PatternType.COMBINED:
                return CombinedPatternDetector(params)
            elif pattern_type == PatternType.ORDERBOOK:
                return OrderbookPatternDetector(params)
            
            logger.warning(f"Детектор паттернов типа {pattern_type} не найден")
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании детектора паттернов типа {pattern_type}: {e}")
            return None

class DivergenceFactory:
    """
    Фабрика для создания детекторов дивергенций
    """
    
    @staticmethod
    def create(indicator_name: str, params: Dict = None) -> Optional[BaseDivergenceDetector]:
        """
        Создание детектора дивергенций
        
        Args:
            indicator_name: Имя индикатора (RSI, MACD и т.д.)
            params: Параметры детектора
            
        Returns:
            BaseDivergenceDetector: Созданный детектор дивергенций или None при ошибке
        """
        try:
            # Подготовка параметров
            params = params or {}
            
            # Создание детектора в зависимости от индикатора
            if indicator_name == 'RSI':
                return RSIDivergenceDetector(params)
            elif indicator_name == 'MACD':
                return MACDDivergenceDetector(params)
            elif indicator_name == 'Stochastic':
                return StochasticDivergenceDetector(params)
            elif indicator_name == 'OBV':
                return OBVDivergenceDetector(params)
            
            logger.warning(f"Детектор дивергенций для индикатора {indicator_name} не найден")
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании детектора дивергенций для индикатора {indicator_name}: {e}")
            return None

class OrderbookAnalyzerFactory:
    """
    Фабрика для создания анализаторов ордербуков
    """
    
    @staticmethod
    def create(analyzer_type: str, params: Dict = None) -> Optional[OrderbookAnalyzer]:
        """
        Создание анализатора ордербуков
        
        Args:
            analyzer_type: Тип анализатора (LiquidityAnalyzer, ImbalanceAnalyzer и т.д.)
            params: Параметры анализатора
            
        Returns:
            OrderbookAnalyzer: Созданный анализатор ордербуков или None при ошибке
        """
        try:
            # Подготовка параметров
            params = params or {}
            
            # Создание анализатора в зависимости от типа
            if analyzer_type == 'LiquidityAnalyzer':
                return LiquidityAnalyzer(params)
            elif analyzer_type == 'ImbalanceAnalyzer':
                return ImbalanceAnalyzer(params)
            elif analyzer_type == 'SupportResistanceAnalyzer':
                return SupportResistanceAnalyzer(params)
            
            logger.warning(f"Анализатор ордербуков типа {analyzer_type} не найден")
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании анализатора ордербуков типа {analyzer_type}: {e}")
            return None

# ===========================================================================
# БЛОК 4: РЕАЛИЗАЦИЯ ТРЕНДОВЫХ ИНДИКАТОРОВ
# ===========================================================================

class MACDIndicator(BaseIndicator):
    """
    Индикатор MACD (Moving Average Convergence Divergence)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора MACD
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'MACD'
        self.type = IndicatorType.TREND
        
        # Настройка параметров
        self.fast_period = self.params.get('fast_period', 12)
        self.slow_period = self.params.get('slow_period', 26)
        self.signal_period = self.params.get('signal_period', 9)
        self.source_column = self.params.get('source_column', 'close')
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета MACD
        
        Returns:
            List[str]: Список колонок
        """
        return [self.source_column]
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет MACD
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (MACD линия, сигнальная линия, гистограмма)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет экспоненциальных скользящих средних
            fast_ema = df[self.source_column].ewm(span=self.fast_period, adjust=False).mean()
            slow_ema = df[self.source_column].ewm(span=self.slow_period, adjust=False).mean()
            
            # Расчет MACD линии (разница между быстрой и медленной EMA)
            macd_line = fast_ema - slow_ema
            
            # Расчет сигнальной линии (EMA от MACD линии)
            signal_line = macd_line.ewm(span=self.signal_period, adjust=False).mean()
            
            # Расчет гистограммы (разница между MACD линией и сигнальной линией)
            histogram = macd_line - signal_line
            
            # Сохраняем результат
            self.result = {
                'macd_line': macd_line,
                'signal_line': signal_line,
                'histogram': histogram
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете MACD: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе MACD
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            macd_line = self.result['macd_line']
            signal_line = self.result['signal_line']
            histogram = self.result['histogram']
            
            # Поиск точек пересечения MACD и сигнальной линии
            crossover = (macd_line > signal_line) & (macd_line.shift() <= signal_line.shift())
            crossunder = (macd_line < signal_line) & (macd_line.shift() >= signal_line.shift())
            
            # Расчет дополнительных параметров для фильтрации сигналов
            is_above_zero = macd_line > 0
            is_below_zero = macd_line < 0
            histogram_gaining = histogram > histogram.shift()
            histogram_declining = histogram < histogram.shift()
            
            # Генерация сигналов
            for i in range(1, len(df)):
                if crossover.iloc[i]:
                    # Сила сигнала зависит от расстояния от нуля и направления гистограммы
                    strength = min(0.5 + abs(macd_line.iloc[i]) / (macd_line.abs().max() * 2), 1.0)
                    if is_above_zero.iloc[i]:
                        strength *= 1.2  # Усиливаем сигнал, если MACD выше нуля
                    if histogram_gaining.iloc[i]:
                        strength *= 1.1  # Усиливаем сигнал, если гистограмма растет
                    
                    # Нормализуем силу сигнала
                    strength = min(strength, 1.0)
                    
                    # Тип сигнала зависит от положения относительно нуля
                    signal_type = SignalType.STRONG_BUY if is_above_zero.iloc[i] else SignalType.BUY
                    
                    # Расчет стоп-лосса (на основе ATR, если доступен)
                    stop_loss = None
                    take_profit = None
                    
                    if 'atr' in df.columns:
                        atr = df['atr'].iloc[i]
                        price = df[self.source_column].iloc[i]
                        stop_loss = price - (atr * 2)  # Стоп-лосс на 2 ATR ниже текущей цены
                        take_profit = price + (atr * 3)  # Тейк-профит на 3 ATR выше текущей цены
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=signal_type,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=df[self.source_column].iloc[i],
                        description=f"MACD пересекает сигнальную линию снизу вверх (значение: {macd_line.iloc[i]:.4f})",
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signals.append(signal)
                
                elif crossunder.iloc[i]:
                    # Сила сигнала зависит от расстояния от нуля и направления гистограммы
                    strength = min(0.5 + abs(macd_line.iloc[i]) / (macd_line.abs().max() * 2), 1.0)
                    if is_below_zero.iloc[i]:
                        strength *= 1.2  # Усиливаем сигнал, если MACD ниже нуля
                    if histogram_declining.iloc[i]:
                        strength *= 1.1  # Усиливаем сигнал, если гистограмма падает
                    
                    # Нормализуем силу сигнала
                    strength = min(strength, 1.0)
                    
                    # Тип сигнала зависит от положения относительно нуля
                    signal_type = SignalType.STRONG_SELL if is_below_zero.iloc[i] else SignalType.SELL
                    
                    # Расчет стоп-лосса (на основе ATR, если доступен)
                    stop_loss = None
                    take_profit = None
                    
                    if 'atr' in df.columns:
                        atr = df['atr'].iloc[i]
                        price = df[self.source_column].iloc[i]
                        stop_loss = price + (atr * 2)  # Стоп-лосс на 2 ATR выше текущей цены
                        take_profit = price - (atr * 3)  # Тейк-профит на 3 ATR ниже текущей цены
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=signal_type,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=df[self.source_column].iloc[i],
                        description=f"MACD пересекает сигнальную линию сверху вниз (значение: {macd_line.iloc[i]:.4f})",
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов MACD: {e}")
            return []

class EMAIndicator(BaseIndicator):
    """
    Индикатор EMA (Exponential Moving Average)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора EMA
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'EMA'
        self.type = IndicatorType.TREND
        
        # Настройка параметров
        self.periods = self.params.get('periods', [9, 21, 50, 200])
        self.source_column = self.params.get('source_column', 'close')
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета EMA
        
        Returns:
            List[str]: Список колонок
        """
        return [self.source_column]
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет EMA для нескольких периодов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета для каждого периода
        """
        if not self.validate_data(df):
            return {}
        
        try:
            result = {}
            
            # Расчет EMA для каждого периода
            for period in self.periods:
                ema = df[self.source_column].ewm(span=period, adjust=False).mean()
                result[f'ema_{period}'] = ema
            
            # Сохраняем результат
            self.result = result
            
            return result
        except Exception as e:
            logger.error(f"Ошибка при расчете EMA: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе EMA
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем цены
            price = df[self.source_column]
            
            # Сортируем периоды
            sorted_periods = sorted(self.periods)
            
            # Генерация сигналов на основе пересечения цены и EMA
            for period in sorted_periods:
                # Получаем EMA для текущего периода
                ema = self.result[f'ema_{period}']
                
                # Поиск точек пересечения цены и EMA
                price_above_ema = price > ema
                crossover = (price > ema) & (price.shift() <= ema.shift())
                crossunder = (price < ema) & (price.shift() >= ema.shift())
                
                # Подсчет количества EMA выше и ниже текущей цены
                periods_above_price = 0
                periods_below_price = 0
                
                for p in sorted_periods:
                    p_ema = self.result[f'ema_{p}']
                    if price.iloc[-1] > p_ema.iloc[-1]:
                        periods_above_price += 1
                    else:
                        periods_below_price += 1
                
                # Генерация сигналов для точек пересечения
                for i in range(1, len(df)):
                    if crossover.iloc[i]:
                        # Сила сигнала зависит от периода и расположения EMA относительно цены
                        strength = 0.5 + 0.5 * (periods_above_price / len(sorted_periods))
                        
                        # Дополнительное усиление для коротких периодов
                        period_weight = 1.0 - (sorted_periods.index(period) / len(sorted_periods))
                        strength *= period_weight
                        
                        # Нормализуем силу сигнала
                        strength = min(strength, 1.0)
                        
                        # Тип сигнала
                        signal_type = SignalType.BUY
                        
                        # Расчет стоп-лосса (на основе расстояния до EMA)
                        price_val = price.iloc[i]
                        ema_val = ema.iloc[i]
                        distance = abs(price_val - ema_val)
                        
                        stop_loss = price_val - distance * 1.5
                        take_profit = price_val + distance * 2.5
                        
                        # Создаем сигнал
                        signal = Signal(
                            type=signal_type,
                            strength=strength,
                            indicator=f"{self.name}_{period}",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=price_val,
                            description=f"Цена пересекает EMA({period}) снизу вверх",
                            stop_loss=stop_loss,
                            take_profit=take_profit
                        )
                        
                        signals.append(signal)
                    
                    elif crossunder.iloc[i]:
                        # Сила сигнала зависит от периода и расположения EMA относительно цены
                        strength = 0.5 + 0.5 * (periods_below_price / len(sorted_periods))
                        
                        # Дополнительное усиление для коротких периодов
                        period_weight = 1.0 - (sorted_periods.index(period) / len(sorted_periods))
                        strength *= period_weight
                        
                        # Нормализуем силу сигнала
                        strength = min(strength, 1.0)
                        
                        # Тип сигнала
                        signal_type = SignalType.SELL
                        
                        # Расчет стоп-лосса (на основе расстояния до EMA)
                        price_val = price.iloc[i]
                        ema_val = ema.iloc[i]
                        distance = abs(price_val - ema_val)
                        
                        stop_loss = price_val + distance * 1.5
                        take_profit = price_val - distance * 2.5
                        
                        # Создаем сигнал
                        signal = Signal(
                            type=signal_type,
                            strength=strength,
                            indicator=f"{self.name}_{period}",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=price_val,
                            description=f"Цена пересекает EMA({period}) сверху вниз",
                            stop_loss=stop_loss,
                            take_profit=take_profit
                        )
                        
                        signals.append(signal)
            
            # Проверка пересечений между разными EMA (например, "Золотой крест" и "Мертвый крест")
            if len(sorted_periods) >= 2:
                fast_period = sorted_periods[0]  # Самый быстрый период
                slow_period = sorted_periods[-1]  # Самый медленный период
                
                fast_ema = self.result[f'ema_{fast_period}']
                slow_ema = self.result[f'ema_{slow_period}']
                
                # Поиск "Золотого креста" (быстрая EMA пересекает медленную снизу вверх)
                golden_cross = (fast_ema > slow_ema) & (fast_ema.shift() <= slow_ema.shift())
                
                # Поиск "Мертвого креста" (быстрая EMA пересекает медленную сверху вниз)
                death_cross = (fast_ema < slow_ema) & (fast_ema.shift() >= slow_ema.shift())
                
                # Генерация сигналов для "Золотого креста" и "Мертвого креста"
                for i in range(1, len(df)):
                    if golden_cross.iloc[i]:
                        # Создаем сигнал для "Золотого креста"
                        signal = Signal(
                            type=SignalType.STRONG_BUY,
                            strength=0.9,  # Сильный сигнал
                            indicator=f"{self.name}_GoldenCross",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=price.iloc[i],
                            description=f"Золотой крест: EMA({fast_period}) пересекает EMA({slow_period}) снизу вверх",
                            stop_loss=price.iloc[i] * 0.95,  # Примерный стоп-лосс на 5% ниже цены
                            take_profit=price.iloc[i] * 1.15  # Примерный тейк-профит на 15% выше цены
                        )
                        
                        signals.append(signal)
                    
                    elif death_cross.iloc[i]:
                        # Создаем сигнал для "Мертвого креста"
                        signal = Signal(
                            type=SignalType.STRONG_SELL,
                            strength=0.9,  # Сильный сигнал
                            indicator=f"{self.name}_DeathCross",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=price.iloc[i],
                            description=f"Мертвый крест: EMA({fast_period}) пересекает EMA({slow_period}) сверху вниз",
                            stop_loss=price.iloc[i] * 1.05,  # Примерный стоп-лосс на 5% выше цены
                            take_profit=price.iloc[i] * 0.85  # Примерный тейк-профит на 15% ниже цены
                        )
                        
                        signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов EMA: {e}")
            return []

class ADXIndicator(BaseIndicator):
    """
    Индикатор ADX (Average Directional Index)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора ADX
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'ADX'
        self.type = IndicatorType.TREND
        
        # Настройка параметров
        self.period = self.params.get('period', 14)
        self.threshold = self.params.get('threshold', 25)
        self.strong_trend = self.params.get('strong_trend', 35)
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета ADX
        
        Returns:
            List[str]: Список колонок
        """
        return ['high', 'low', 'close']
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет ADX
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (ADX, +DI, -DI)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет True Range (TR)
            high_low = df['high'] - df['low']
            high_close = (df['high'] - df['close'].shift()).abs()
            low_close = (df['low'] - df['close'].shift()).abs()
            
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            
            # Сглаживание TR для расчета ATR
            atr = true_range.ewm(alpha=1/self.period, adjust=False).mean()
            
            # Расчет +DM и -DM
            plus_dm = df['high'].diff()
            minus_dm = df['low'].shift().diff(-1)
            
            # Условия для +DM и -DM
            plus_dm = pd.Series(
                np.where(
                    (plus_dm > 0) & (plus_dm > minus_dm.abs()),
                    plus_dm,
                    0
                ),
                index=df.index
            )
            
            minus_dm = pd.Series(
                np.where(
                    (minus_dm > 0) & (minus_dm > plus_dm.abs()),
                    minus_dm,
                    0
                ),
                index=df.index
            )
            
            # Сглаживание +DM и -DM
            plus_di = 100 * plus_dm.ewm(alpha=1/self.period, adjust=False).mean() / atr
            minus_di = 100 * minus_dm.ewm(alpha=1/self.period, adjust=False).mean() / atr
            
            # Расчет направленного индекса (DX)
            di_diff = (plus_di - minus_di).abs()
            di_sum = plus_di + minus_di
            dx = 100 * di_diff / di_sum
            
            # Сглаживание DX для расчета ADX
            adx = dx.ewm(alpha=1/self.period, adjust=False).mean()
            
            # Сохраняем результат
            self.result = {
                'adx': adx,
                'plus_di': plus_di,
                'minus_di': minus_di
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете ADX: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе ADX
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            adx = self.result['adx']
            plus_di = self.result['plus_di']
            minus_di = self.result['minus_di']
            
            # Поиск точек пересечения +DI и -DI
            plus_di_cross_above = (plus_di > minus_di) & (plus_di.shift() <= minus_di.shift())
            plus_di_cross_below = (plus_di < minus_di) & (plus_di.shift() >= minus_di.shift())
            
            # Проверка силы тренда по ADX
            strong_trend = adx > self.strong_trend
            medium_trend = (adx > self.threshold) & (adx <= self.strong_trend)
            weak_trend = adx <= self.threshold
            
            # Генерация сигналов
            for i in range(1, len(df)):
                if plus_di_cross_above.iloc[i]:
                    # Сила сигнала зависит от значения ADX
                    if strong_trend.iloc[i]:
                        strength = 0.9
                        signal_type = SignalType.STRONG_BUY
                    elif medium_trend.iloc[i]:
                        strength = 0.7
                        signal_type = SignalType.BUY
                    else:
                        strength = 0.5
                        signal_type = SignalType.WEAK_BUY
                    
                    # Расчет стоп-лосса и тейк-профита
                    price = df['close'].iloc[i]
                    atr_value = 0
                    
                    if 'atr' in df.columns:
                        atr_value = df['atr'].iloc[i]
                    elif 'tr' in self.result:
                        atr_value = self.result['tr'].iloc[i]
                    else:
                        # Приблизительный расчет ATR
                        high_low = df['high'].iloc[i] - df['low'].iloc[i]
                        high_close = abs(df['high'].iloc[i] - df['close'].iloc[i-1])
                        low_close = abs(df['low'].iloc[i] - df['close'].iloc[i-1])
                        atr_value = max(high_low, high_close, low_close)
                    
                    stop_loss = price - (atr_value * 2)
                    take_profit = price + (atr_value * 3)
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=signal_type,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"+DI пересекает -DI снизу вверх, ADX: {adx.iloc[i]:.2f}",
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signals.append(signal)
                
                elif plus_di_cross_below.iloc[i]:
                    # Сила сигнала зависит от значения ADX
                    if strong_trend.iloc[i]:
                        strength = 0.9
                        signal_type = SignalType.STRONG_SELL
                    elif medium_trend.iloc[i]:
                        strength = 0.7
                        signal_type = SignalType.SELL
                    else:
                        strength = 0.5
                        signal_type = SignalType.WEAK_SELL
                    
                    # Расчет стоп-лосса и тейк-профита
                    price = df['close'].iloc[i]
                    atr_value = 0
                    
                    if 'atr' in df.columns:
                        atr_value = df['atr'].iloc[i]
                    elif 'tr' in self.result:
                        atr_value = self.result['tr'].iloc[i]
                    else:
                        # Приблизительный расчет ATR
                        high_low = df['high'].iloc[i] - df['low'].iloc[i]
                        high_close = abs(df['high'].iloc[i] - df['close'].iloc[i-1])
                        low_close = abs(df['low'].iloc[i] - df['close'].iloc[i-1])
                        atr_value = max(high_low, high_close, low_close)
                    
                    stop_loss = price + (atr_value * 2)
                    take_profit = price - (atr_value * 3)
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=signal_type,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"+DI пересекает -DI сверху вниз, ADX: {adx.iloc[i]:.2f}",
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов ADX: {e}")
            return []

# ===========================================================================
# БЛОК 5: РЕАЛИЗАЦИЯ ИНДИКАТОРОВ МОМЕНТУМА
# ===========================================================================

class RSIIndicator(BaseIndicator):
    """
    Индикатор RSI (Relative Strength Index)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора RSI
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'RSI'
        self.type = IndicatorType.MOMENTUM
        
        # Настройка параметров
        self.period = self.params.get('period', 14)
        self.overbought = self.params.get('overbought', 70)
        self.oversold = self.params.get('oversold', 30)
        self.source_column = self.params.get('source_column', 'close')
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета RSI
        
        Returns:
            List[str]: Список колонок
        """
        return [self.source_column]
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет RSI
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (RSI)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет изменений цены
            delta = df[self.source_column].diff()
            
            # Разделение положительных и отрицательных изменений
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            # Расчет среднего прироста и убытка (с использованием EMA для сглаживания)
            # Используем формулу Вальдера для правильного расчета
            avg_gain = gain.ewm(alpha=1.0/self.period, min_periods=self.period, adjust=False).mean()
            avg_loss = loss.ewm(alpha=1.0/self.period, min_periods=self.period, adjust=False).mean()
            
            # Расчет относительной силы (RS)
            rs = avg_gain / avg_loss
            
            # Расчет RSI
            rsi = 100 - (100 / (1 + rs))
            
            # Сохраняем результат
            self.result = {
                'rsi': rsi
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете RSI: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе RSI
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем рассчитанный RSI
            rsi = self.result['rsi']
            
            # Поиск точек перекупленности и перепроданности
            overbought = rsi > self.overbought
            oversold = rsi < self.oversold
            
            # Поиск точек выхода из зон перекупленности и перепроданности
            exit_overbought = (rsi < self.overbought) & (rsi.shift() >= self.overbought)
            exit_oversold = (rsi > self.oversold) & (rsi.shift() <= self.oversold)
            
            # Поиск точек разворота (локальные экстремумы)
            local_min = (rsi < rsi.shift()) & (rsi < rsi.shift(-1))
            local_max = (rsi > rsi.shift()) & (rsi > rsi.shift(-1))
            
            # Генерация сигналов
            for i in range(1, len(df) - 1):
                price = df[self.source_column].iloc[i]
                
                # Сигнал перепроданности (потенциальная покупка)
                if oversold.iloc[i] and local_min.iloc[i]:
                    # Сила сигнала зависит от того, насколько сильно перепродан RSI
                    strength = 0.5 + 0.5 * (1 - (rsi.iloc[i] / self.oversold))
                    
                    # Нормализуем силу сигнала
                    strength = min(max(strength, 0.1), 1.0)
                    
                    # Определяем тип сигнала в зависимости от силы
                    if strength > 0.8:
                        signal_type = SignalType.STRONG_BUY
                    elif strength > 0.5:
                        signal_type = SignalType.BUY
                    else:
                        signal_type = SignalType.WEAK_BUY
                    
                    # Расчет стоп-лосса и тейк-профита
                    atr_value = 0
                    if 'atr' in df.columns:
                        atr_value = df['atr'].iloc[i]
                    else:
                        # Приблизительный расчет волатильности
                        atr_value = (df['high'].iloc[i] - df['low'].iloc[i]) * 0.5
                    
                    stop_loss = price * 0.98  # Стоп-лосс на 2% ниже
                    take_profit = price * 1.04  # Тейк-профит на 4% выше
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=signal_type,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"RSI в зоне перепроданности: {rsi.iloc[i]:.2f}",
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signals.append(signal)
                
                # Сигнал перекупленности (потенциальная продажа)
                elif overbought.iloc[i] and local_max.iloc[i]:
                    # Сила сигнала зависит от того, насколько сильно перекуплен RSI
                    strength = 0.5 + 0.5 * ((rsi.iloc[i] - self.overbought) / (100 - self.overbought))
                    
                    # Нормализуем силу сигнала
                    strength = min(max(strength, 0.1), 1.0)
                    
                    # Определяем тип сигнала в зависимости от силы
                    if strength > 0.8:
                        signal_type = SignalType.STRONG_SELL
                    elif strength > 0.5:
                        signal_type = SignalType.SELL
                    else:
                        signal_type = SignalType.WEAK_SELL
                    
                    # Расчет стоп-лосса и тейк-профита
                    atr_value = 0
                    if 'atr' in df.columns:
                        atr_value = df['atr'].iloc[i]
                    else:
                        # Приблизительный расчет волатильности
                        atr_value = (df['high'].iloc[i] - df['low'].iloc[i]) * 0.5
                    
                    stop_loss = price * 1.02  # Стоп-лосс на 2% выше
                    take_profit = price * 0.96  # Тейк-профит на 4% ниже
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=signal_type,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"RSI в зоне перекупленности: {rsi.iloc[i]:.2f}",
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    signals.append(signal)
                
                # Сигнал выхода из зоны перепроданности
                elif exit_oversold.iloc[i]:
                    # Сила сигнала
                    strength = 0.7
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=strength,
                        indicator=f"{self.name}_ExitOversold",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"RSI выходит из зоны перепроданности: {rsi.iloc[i]:.2f}",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Сигнал выхода из зоны перекупленности
                elif exit_overbought.iloc[i]:
                    # Сила сигнала
                    strength = 0.7
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=strength,
                        indicator=f"{self.name}_ExitOverbought",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"RSI выходит из зоны перекупленности: {rsi.iloc[i]:.2f}",
                        stop_loss=price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов RSI: {e}")
            return []

class StochasticIndicator(BaseIndicator):
    """
    Стохастический осциллятор
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация Стохастического осциллятора
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'Stochastic'
        self.type = IndicatorType.MOMENTUM
        
        # Настройка параметров
        self.k_period = self.params.get('k_period', 14)
        self.d_period = self.params.get('d_period', 3)
        self.smooth_k = self.params.get('smooth_k', 3)
        self.overbought = self.params.get('overbought', 80)
        self.oversold = self.params.get('oversold', 20)
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета Стохастического осциллятора
        
        Returns:
            List[str]: Список колонок
        """
        return ['high', 'low', 'close']
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет Стохастического осциллятора
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (%K и %D)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет наивысшего максимума и наименьшего минимума за период
            highest_high = df['high'].rolling(window=self.k_period).max()
            lowest_low = df['low'].rolling(window=self.k_period).min()
            
            # Расчет %K (основная линия)
            k_raw = 100 * ((df['close'] - lowest_low) / (highest_high - lowest_low))
            
            # Сглаживание %K (если включено)
            if self.smooth_k > 1:
                k = k_raw.rolling(window=self.smooth_k).mean()
            else:
                k = k_raw
            
            # Расчет %D (сигнальная линия)
            d = k.rolling(window=self.d_period).mean()
            
            # Сохраняем результат
            self.result = {
                'k': k,
                'd': d
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете Stochastic: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе Стохастического осциллятора
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем рассчитанные линии %K и %D
            k = self.result['k']
            d = self.result['d']
            
            # Поиск точек пересечения %K и %D
            k_cross_above_d = (k > d) & (k.shift() <= d.shift())
            k_cross_below_d = (k < d) & (k.shift() >= d.shift())
            
            # Поиск зон перекупленности и перепроданности
            overbought_zone = (k > self.overbought) & (d > self.overbought)
            oversold_zone = (k < self.oversold) & (d < self.oversold)
            
            # Поиск выхода из зон перекупленности и перепроданности
            exit_overbought = (~overbought_zone) & (overbought_zone.shift())
            exit_oversold = (~oversold_zone) & (oversold_zone.shift())
            
            # Поиск бычьей/медвежьей дивергенции (простой вариант)
            # Это будет реализовано в отдельном классе дивергенций
            
            # Генерация сигналов
            for i in range(1, len(df)):
                price = df['close'].iloc[i]
                
                # Пересечение %K и %D в зоне перепроданности (покупка)
                if k_cross_above_d.iloc[i] and oversold_zone.iloc[i]:
                    # Сила сигнала
                    strength = 0.8
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.STRONG_BUY,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"%K пересекает %D снизу вверх в зоне перепроданности",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Пересечение %K и %D в зоне перекупленности (продажа)
                elif k_cross_below_d.iloc[i] and overbought_zone.iloc[i]:
                    # Сила сигнала
                    strength = 0.8
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.STRONG_SELL,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"%K пересекает %D сверху вниз в зоне перекупленности",
                        stop_loss=price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
                
                # Пересечение %K и %D в нейтральной зоне
                elif k_cross_above_d.iloc[i] and not overbought_zone.iloc[i] and not oversold_zone.iloc[i]:
                    # Сила сигнала
                    strength = 0.5
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"%K пересекает %D снизу вверх в нейтральной зоне",
                        stop_loss=price * 0.98,  # Стоп-лосс на 2% ниже
                        take_profit=price * 1.04  # Тейк-профит на 4% выше
                    )
                    
                    signals.append(signal)
                
                elif k_cross_below_d.iloc[i] and not overbought_zone.iloc[i] and not oversold_zone.iloc[i]:
                    # Сила сигнала
                    strength = 0.5
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=strength,
                        indicator=self.name,
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"%K пересекает %D сверху вниз в нейтральной зоне",
                        stop_loss=price * 1.02,  # Стоп-лосс на 2% выше
                        take_profit=price * 0.96  # Тейк-профит на 4% ниже
                    )
                    
                    signals.append(signal)
                
                # Выход из зоны перепроданности
                elif exit_oversold.iloc[i]:
                    # Сила сигнала
                    strength = 0.7
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=strength,
                        indicator=f"{self.name}_ExitOversold",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Выход из зоны перепроданности стохастика",
                        stop_loss=price * 0.98,  # Стоп-лосс на 2% ниже
                        take_profit=price * 1.05  # Тейк-профит на 5% выше
                    )
                    
                    signals.append(signal)
                
                # Выход из зоны перекупленности
                elif exit_overbought.iloc[i]:
                    # Сила сигнала
                    strength = 0.7
                    
                    # Создаем сигнал
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=strength,
                        indicator=f"{self.name}_ExitOverbought",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Выход из зоны перекупленности стохастика",
                        stop_loss=price * 1.02,  # Стоп-лосс на 2% выше
                        take_profit=price * 0.95  # Тейк-профит на 5% ниже
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов Stochastic: {e}")
            return []

# ===========================================================================
# БЛОК 6: РЕАЛИЗАЦИЯ ИНДИКАТОРОВ ВОЛАТИЛЬНОСТИ
# ===========================================================================

class ATRIndicator(BaseIndicator):
    """
    Индикатор ATR (Average True Range)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора ATR
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'ATR'
        self.type = IndicatorType.VOLATILITY
        
        # Настройка параметров
        self.period = self.params.get('period', 14)
        self.smoothing = self.params.get('smoothing', 'ewm')  # 'ewm' или 'sma'
        self.multiplier = self.params.get('multiplier', 1.0)  # Мультипликатор для расчета торговых диапазонов
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета ATR
        
        Returns:
            List[str]: Список колонок
        """
        return ['high', 'low', 'close']
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет ATR
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (ATR и TR)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет True Range (TR)
            high_low = df['high'] - df['low']
            high_close = (df['high'] - df['close'].shift()).abs()
            low_close = (df['low'] - df['close'].shift()).abs()
            
            # Максимальное значение из трех
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            tr = ranges.max(axis=1)
            
            # Расчет ATR
            if self.smoothing == 'ewm':
                # Экспоненциальное скользящее среднее
                atr = tr.ewm(span=self.period, adjust=False).mean()
            else:
                # Простое скользящее среднее
                atr = tr.rolling(window=self.period).mean()
            
            # Расчет торговых диапазонов (уровни поддержки и сопротивления)
            upper_band = df['close'] + (atr * self.multiplier)
            lower_band = df['close'] - (atr * self.multiplier)
            
            # Сохраняем результат
            self.result = {
                'tr': tr,
                'atr': atr,
                'upper_band': upper_band,
                'lower_band': lower_band
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете ATR: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе ATR
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            atr = self.result['atr']
            upper_band = self.result['upper_band']
            lower_band = self.result['lower_band']
            
            # Аномальные значения ATR (резкие изменения волатильности)
            atr_avg = atr.rolling(window=self.period).mean()
            atr_std = atr.rolling(window=self.period).std()
            
            # Порог для аномально высокой волатильности
            high_volatility = atr > (atr_avg + 2 * atr_std)
            
            # Пересечение цены с торговыми диапазонами
            price_cross_upper = (df['close'] > upper_band) & (df['close'].shift() <= upper_band.shift())
            price_cross_lower = (df['close'] < lower_band) & (df['close'].shift() >= lower_band.shift())
            
            # Значительное изменение ATR (>30% за один период)
            significant_atr_change = (atr / atr.shift() - 1).abs() > 0.3
            
            # Генерация сигналов для аномальной волатильности
            for i in range(1, len(df)):
                price = df['close'].iloc[i]
                current_atr = atr.iloc[i]
                
                # Аномально высокая волатильность
                if high_volatility.iloc[i]:
                    # Создаем предупреждающий сигнал о высокой волатильности
                    signal = Signal(
                        type=SignalType.UNDEFINED,  # Нейтральный сигнал (только предупреждение)
                        strength=0.8,
                        indicator=f"{self.name}_HighVolatility",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Аномально высокая волатильность: ATR = {current_atr:.6f}"
                    )
                    
                    signals.append(signal)
                
                # Значительное изменение ATR
                if significant_atr_change.iloc[i]:
                    # Создаем предупреждающий сигнал о резком изменении волатильности
                    change_percent = ((atr.iloc[i] / atr.iloc[i-1]) - 1) * 100
                    change_direction = "увеличение" if change_percent > 0 else "уменьшение"
                    
                    signal = Signal(
                        type=SignalType.UNDEFINED,  # Нейтральный сигнал (только предупреждение)
                        strength=0.7,
                        indicator=f"{self.name}_VolatilityChange",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Резкое {change_direction} волатильности: {abs(change_percent):.2f}%"
                    )
                    
                    signals.append(signal)
                
                # Пробой верхней границы
                if price_cross_upper.iloc[i]:
                    # Создаем сигнал о пробое верхней границы (может быть использован для стоп-лосса)
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=0.6,
                        indicator=f"{self.name}_UpperBreak",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Пробой верхней границы ATR ({self.multiplier}x)",
                        stop_loss=price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
                
                # Пробой нижней границы
                if price_cross_lower.iloc[i]:
                    # Создаем сигнал о пробое нижней границы (может быть использован для стоп-лосса)
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.6,
                        indicator=f"{self.name}_LowerBreak",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Пробой нижней границы ATR ({self.multiplier}x)",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов ATR: {e}")
            return []

class BollingerBandsIndicator(BaseIndicator):
    """
    Индикатор Bollinger Bands
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора Bollinger Bands
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'Bollinger'
        self.type = IndicatorType.VOLATILITY
        
        # Настройка параметров
        self.period = self.params.get('period', 20)
        self.std_dev = self.params.get('std_dev', 2)
        self.source_column = self.params.get('source_column', 'close')
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета Bollinger Bands
        
        Returns:
            List[str]: Список колонок
        """
        return [self.source_column]
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет Bollinger Bands
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (верхняя, средняя и нижняя полосы)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет средней линии (SMA)
            middle_band = df[self.source_column].rolling(window=self.period).mean()
            
            # Расчет стандартного отклонения
            std = df[self.source_column].rolling(window=self.period).std()
            
            # Расчет верхней и нижней полос
            upper_band = middle_band + (std * self.std_dev)
            lower_band = middle_band - (std * self.std_dev)
            
            # Расчет ширины полос (полезно для определения сжатия/расширения)
            bandwidth = (upper_band - lower_band) / middle_band
            
            # Расчет %B (положение цены относительно полос)
            percent_b = (df[self.source_column] - lower_band) / (upper_band - lower_band)
            
            # Сохраняем результат
            self.result = {
                'upper': upper_band,
                'middle': middle_band,
                'lower': lower_band,
                'bandwidth': bandwidth,
                'percent_b': percent_b
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете Bollinger Bands: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе Bollinger Bands
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            upper_band = self.result['upper']
            middle_band = self.result['middle']
            lower_band = self.result['lower']
            bandwidth = self.result['bandwidth']
            percent_b = self.result['percent_b']
            
            # Проверка на выход цены за границы полос
            price = df[self.source_column]
            price_above_upper = price > upper_band
            price_below_lower = price < lower_band
            
            # Проверка на возврат цены внутрь полос после выхода
            reentry_from_above = (price <= upper_band) & (price.shift() > upper_band.shift())
            reentry_from_below = (price >= lower_band) & (price.shift() < lower_band.shift())
            
            # Обнаружение сужения полос (Bollinger Squeeze)
            # Используем скользящее среднее диапазона для нормализации
            bandwidth_ma = bandwidth.rolling(window=50).mean()
            squeeze = bandwidth < (bandwidth_ma * 0.5)
            
            # Обнаружение расширения полос после сужения
            expansion_after_squeeze = (~squeeze) & (squeeze.shift())
            
            # W-образное дно (W-Bottom) - паттерн разворота
            # Проверяем два последовательных касания нижней полосы с отскоком
            w_bottom_check = []
            for i in range(2, len(df) - 2):
                if (price.iloc[i-2] > lower_band.iloc[i-2] and
                    price.iloc[i-1] <= lower_band.iloc[i-1] and
                    price.iloc[i] > lower_band.iloc[i] and
                    price.iloc[i+1] <= lower_band.iloc[i+1] and
                    price.iloc[i+2] > lower_band.iloc[i+2]):
                    w_bottom_check.append(i+2)
            
            # M-образная вершина (M-Top) - паттерн разворота
            # Проверяем два последовательных касания верхней полосы с отскоком
            m_top_check = []
            for i in range(2, len(df) - 2):
                if (price.iloc[i-2] < upper_band.iloc[i-2] and
                    price.iloc[i-1] >= upper_band.iloc[i-1] and
                    price.iloc[i] < upper_band.iloc[i] and
                    price.iloc[i+1] >= upper_band.iloc[i+1] and
                    price.iloc[i+2] < upper_band.iloc[i+2]):
                    m_top_check.append(i+2)
            
            # Генерация сигналов
            for i in range(1, len(df)):
                current_price = price.iloc[i]
                
                # Сигнал при сужении полос (предупреждение о потенциальном сильном движении)
                if squeeze.iloc[i] and not squeeze.iloc[i-1]:
                    signal = Signal(
                        type=SignalType.UNDEFINED,
                        strength=0.7,
                        indicator=f"{self.name}_Squeeze",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"Bollinger Squeeze: сужение полос, ожидается сильное движение"
                    )
                    
                    signals.append(signal)
                
                # Сигнал при расширении полос после сужения (подтверждение начала движения)
                if expansion_after_squeeze.iloc[i]:
                    # Определяем направление движения
                    direction = "вверх" if price.iloc[i] > price.iloc[i-1] else "вниз"
                    signal_type = SignalType.BUY if direction == "вверх" else SignalType.SELL
                    
                    signal = Signal(
                        type=signal_type,
                        strength=0.8,
                        indicator=f"{self.name}_Expansion",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"Расширение после Bollinger Squeeze: движение {direction}",
                        stop_loss=current_price * 0.97 if direction == "вверх" else current_price * 1.03,
                        take_profit=current_price * 1.06 if direction == "вверх" else current_price * 0.94
                    )
                    
                    signals.append(signal)
                
                # Сигнал при возврате цены внутрь полос после выхода за верхнюю границу
                if reentry_from_above.iloc[i]:
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=0.6,
                        indicator=f"{self.name}_UpperReentry",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"Возврат цены внутрь Bollinger Bands после пробоя верхней полосы",
                        stop_loss=current_price * 1.02,  # Стоп-лосс на 2% выше
                        take_profit=current_price * 0.96  # Тейк-профит на 4% ниже
                    )
                    
                    signals.append(signal)
                
                # Сигнал при возврате цены внутрь полос после выхода за нижнюю границу
                if reentry_from_below.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.6,
                        indicator=f"{self.name}_LowerReentry",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"Возврат цены внутрь Bollinger Bands после пробоя нижней полосы",
                        stop_loss=current_price * 0.98,  # Стоп-лосс на 2% ниже
                        take_profit=current_price * 1.04  # Тейк-профит на 4% выше
                    )
                    
                    signals.append(signal)
                
                # Сигналы для W-Bottom и M-Top
                if i in w_bottom_check:
                    signal = Signal(
                        type=SignalType.STRONG_BUY,
                        strength=0.9,
                        indicator=f"{self.name}_WBottom",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"W-образное дно (Bollinger W-Bottom)",
                        stop_loss=current_price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=current_price * 1.09  # Тейк-профит на 9% выше
                    )
                    
                    signals.append(signal)
                
                if i in m_top_check:
                    signal = Signal(
                        type=SignalType.STRONG_SELL,
                        strength=0.9,
                        indicator=f"{self.name}_MTop",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"M-образная вершина (Bollinger M-Top)",
                        stop_loss=current_price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=current_price * 0.91  # Тейк-профит на 9% ниже
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов Bollinger Bands: {e}")
            return []

# ===========================================================================
# БЛОК 7: РЕАЛИЗАЦИЯ ОБЪЕМНЫХ ИНДИКАТОРОВ
# ===========================================================================

class OBVIndicator(BaseIndicator):
    """
    Индикатор OBV (On-Balance Volume)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора OBV
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'OBV'
        self.type = IndicatorType.VOLUME
        
        # Настройка параметров
        self.ema_period = self.params.get('ema_period', 20)  # Период для EMA от OBV
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета OBV
        
        Returns:
            List[str]: Список колонок
        """
        return ['close', 'volume']
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет OBV
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета (OBV и EMA от OBV)
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Инициализация OBV
            obv = pd.Series(0, index=df.index)
            
            # Расчет OBV
            for i in range(1, len(df)):
                if df['close'].iloc[i] > df['close'].iloc[i-1]:
                    obv.iloc[i] = obv.iloc[i-1] + df['volume'].iloc[i]
                elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                    obv.iloc[i] = obv.iloc[i-1] - df['volume'].iloc[i]
                else:
                    obv.iloc[i] = obv.iloc[i-1]
            
            # Расчет EMA от OBV
            obv_ema = obv.ewm(span=self.ema_period, adjust=False).mean()
            
            # Сохраняем результат
            self.result = {
                'obv': obv,
                'obv_ema': obv_ema
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете OBV: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе OBV
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            obv = self.result['obv']
            obv_ema = self.result['obv_ema']
            
            # Определение дивергенций между ценой и OBV
            price = df['close']
            
            # Пересечение OBV и его EMA
            obv_cross_above_ema = (obv > obv_ema) & (obv.shift() <= obv_ema.shift())
            obv_cross_below_ema = (obv < obv_ema) & (obv.shift() >= obv_ema.shift())
            
            # Аномальный рост объема (более 200% от среднего за последние 20 периодов)
            volume_avg = df['volume'].rolling(window=20).mean()
            high_volume = df['volume'] > (volume_avg * 2)
            
            # Генерация сигналов
            for i in range(1, len(df)):
                current_price = price.iloc[i]
                
                # Пересечение OBV и его EMA (снизу вверх)
                if obv_cross_above_ema.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.7,
                        indicator=f"{self.name}_CrossAbove",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"OBV пересекает свою EMA снизу вверх",
                        stop_loss=current_price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=current_price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Пересечение OBV и его EMA (сверху вниз)
                if obv_cross_below_ema.iloc[i]:
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=0.7,
                        indicator=f"{self.name}_CrossBelow",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=current_price,
                        description=f"OBV пересекает свою EMA сверху вниз",
                        stop_loss=current_price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=current_price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
                
                # Аномальный объем
                if high_volume.iloc[i]:
                    # Определяем направление движения цены
                    if price.iloc[i] > price.iloc[i-1]:
                        signal = Signal(
                            type=SignalType.BUY,
                            strength=0.8,
                            indicator=f"{self.name}_HighVolume",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=current_price,
                            description=f"Аномально высокий объем при растущей цене",
                            stop_loss=current_price * 0.96,  # Стоп-лосс на 4% ниже
                            take_profit=current_price * 1.08  # Тейк-профит на 8% выше
                        )
                    else:
                        signal = Signal(
                            type=SignalType.SELL,
                            strength=0.8,
                            indicator=f"{self.name}_HighVolume",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=current_price,
                            description=f"Аномально высокий объем при падающей цене",
                            stop_loss=current_price * 1.04,  # Стоп-лосс на 4% выше
                            take_profit=current_price * 0.92  # Тейк-профит на 8% ниже
                        )
                    
                    signals.append(signal)
                
                # Поиск примитивных дивергенций (будет улучшено в отдельном классе дивергенций)
                if i >= 5:
                    # Бычья дивергенция: цена падает, OBV растет
                    if (price.iloc[i] < price.iloc[i-5]) and (obv.iloc[i] > obv.iloc[i-5]):
                        signal = Signal(
                            type=SignalType.BUY,
                            strength=0.85,
                            indicator=f"{self.name}_BullishDivergence",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=current_price,
                            description=f"Бычья дивергенция между ценой и OBV",
                            stop_loss=current_price * 0.97,  # Стоп-лосс на 3% ниже
                            take_profit=current_price * 1.09  # Тейк-профит на 9% выше
                        )
                        
                        signals.append(signal)
                    
                    # Медвежья дивергенция: цена растет, OBV падает
                    if (price.iloc[i] > price.iloc[i-5]) and (obv.iloc[i] < obv.iloc[i-5]):
                        signal = Signal(
                            type=SignalType.SELL,
                            strength=0.85,
                            indicator=f"{self.name}_BearishDivergence",
                            timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                            timeframe=self.params.get('timeframe', ''),
                            price=current_price,
                            description=f"Медвежья дивергенция между ценой и OBV",
                            stop_loss=current_price * 1.03,  # Стоп-лосс на 3% выше
                            take_profit=current_price * 0.91  # Тейк-профит на 9% ниже
                        )
                        
                        signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов OBV: {e}")
            return []

class DeltaVolumeIndicator(BaseIndicator):
    """
    Индикатор Delta Volume (аппроксимация)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора Delta Volume
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'Delta'
        self.type = IndicatorType.VOLUME
        
        # Настройка параметров
        self.smoothing_period = self.params.get('smoothing_period', 5)
        self.bull_bias = self.params.get('bull_bias', 0.7)  # Предполагаемая доля покупок в объеме для бычьих свечей
        self.bear_bias = self.params.get('bear_bias', 0.7)  # Предполагаемая доля продаж в объеме для медвежьих свечей
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета Delta Volume
        
        Returns:
            List[str]: Список колонок
        """
        return ['open', 'close', 'volume']
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет Delta Volume (аппроксимация на основе OHLCV данных)
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Определение бычьих и медвежьих свечей
            bullish = df['close'] >= df['open']
            
            # Аппроксимация покупок и продаж на основе типа свечи
            buy_volume = pd.Series(0.0, index=df.index)
            sell_volume = pd.Series(0.0, index=df.index)
            
            # Для бычьих свечей: bull_bias объема - покупки, остальное - продажи
            buy_volume[bullish] = df['volume'][bullish] * self.bull_bias
            sell_volume[bullish] = df['volume'][bullish] * (1 - self.bull_bias)
            
            # Для медвежьих свечей: bear_bias объема - продажи, остальное - покупки
            buy_volume[~bullish] = df['volume'][~bullish] * (1 - self.bear_bias)
            sell_volume[~bullish] = df['volume'][~bullish] * self.bear_bias
            
            # Расчет дельты объема (разница между покупками и продажами)
            delta = buy_volume - sell_volume
            
            # Сглаживание дельты объема
            smoothed_delta = delta.rolling(window=self.smoothing_period).mean()
            
            # Расчет кумулятивной дельты
            cumulative_delta = delta.cumsum()
            
            # Сохраняем результат
            self.result = {
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'delta': delta,
                'smoothed_delta': smoothed_delta,
                'cumulative_delta': cumulative_delta
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете Delta Volume: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе Delta Volume
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            delta = self.result['delta']
            smoothed_delta = self.result['smoothed_delta']
            cumulative_delta = self.result['cumulative_delta']
            
            # Определение изменения направления кумулятивной дельты
            cum_delta_increasing = cumulative_delta > cumulative_delta.shift()
            cum_delta_decreasing = cumulative_delta < cumulative_delta.shift()
            
            # Изменение направления сглаженной дельты
            delta_change_to_positive = (smoothed_delta > 0) & (smoothed_delta.shift() <= 0)
            delta_change_to_negative = (smoothed_delta < 0) & (smoothed_delta.shift() >= 0)
            
            # Аномальные значения дельты (более 3 стандартных отклонений)
            delta_mean = delta.rolling(window=20).mean()
            delta_std = delta.rolling(window=20).std()
            delta_high_positive = delta > (delta_mean + 3 * delta_std)
            delta_high_negative = delta < (delta_mean - 3 * delta_std)
            
            # Генерация сигналов
            for i in range(1, len(df)):
                price = df['close'].iloc[i]
                
                # Изменение направления сглаженной дельты на положительное
                if delta_change_to_positive.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.7,
                        indicator=f"{self.name}_ChangeToPositive",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Изменение направления Delta Volume на положительное",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Изменение направления сглаженной дельты на отрицательное
                if delta_change_to_negative.iloc[i]:
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=0.7,
                        indicator=f"{self.name}_ChangeToNegative",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Изменение направления Delta Volume на отрицательное",
                        stop_loss=price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
                
                # Аномально высокая положительная дельта
                if delta_high_positive.iloc[i]:
                    signal = Signal(
                        type=SignalType.STRONG_BUY,
                        strength=0.85,
                        indicator=f"{self.name}_HighPositive",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Аномально высокая положительная Delta Volume",
                        stop_loss=price * 0.96,  # Стоп-лосс на 4% ниже
                        take_profit=price * 1.08  # Тейк-профит на 8% выше
                    )
                    
                    signals.append(signal)
                
                # Аномально высокая отрицательная дельта
                if delta_high_negative.iloc[i]:
                    signal = Signal(
                        type=SignalType.STRONG_SELL,
                        strength=0.85,
                        indicator=f"{self.name}_HighNegative",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"Аномально высокая отрицательная Delta Volume",
                        stop_loss=price * 1.04,  # Стоп-лосс на 4% выше
                        take_profit=price * 0.92  # Тейк-профит на 8% ниже
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов Delta Volume: {e}")
            return []

class VSAIndicator(BaseIndicator):
    """
    Индикатор VSA (Volume Spread Analysis)
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация индикатора VSA
        
        Args:
            params: Параметры индикатора
        """
        super().__init__(params)
        self.name = 'VSA'
        self.type = IndicatorType.VOLUME
        
        # Настройка параметров
        self.volume_threshold = self.params.get('volume_threshold', 1.5)  # Порог для высокого объема
        self.climax_detection = self.params.get('climax_detection', True)  # Обнаружение кульминаций
        self.lookback_period = self.params.get('lookback_period', 20)  # Период для анализа
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для расчета VSA
        
        Returns:
            List[str]: Список колонок
        """
        return ['open', 'high', 'low', 'close', 'volume']
    
    def calculate(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Расчет VSA паттернов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Dict: Словарь с результатами расчета
        """
        if not self.validate_data(df):
            return {}
        
        try:
            # Расчет спреда (размер свечи)
            spread = df['high'] - df['low']
            
            # Нормализация объема (по отношению к среднему за период)
            volume_ma = df['volume'].rolling(window=self.lookback_period).mean()
            relative_volume = df['volume'] / volume_ma
            
            # Определение типа свечи
            close_loc = (df['close'] - df['low']) / (df['high'] - df['low'])  # Положение закрытия в диапазоне свечи
            is_up_bar = df['close'] > df['open']
            is_down_bar = df['close'] < df['open']
            is_wide_range = spread > spread.rolling(window=self.lookback_period).mean()
            is_narrow_range = spread < spread.rolling(window=self.lookback_period).mean()
            
            # VSA паттерны
            
            # 1. Stopping Volume (останавливающий объем)
            # Высокий объем, широкий спред, цена закрывается в нижней части диапазона
            stopping_volume = (relative_volume > self.volume_threshold) & is_wide_range & (close_loc < 0.3) & is_down_bar
            
            # 2. Climax Volume (объемная кульминация)
            # Очень высокий объем, очень широкий спред, часто на экстремумах
            if self.climax_detection:
                climax_bars = (relative_volume > self.volume_threshold * 1.5) & (spread > spread.rolling(window=self.lookback_period).mean() * 1.5)
                selling_climax = climax_bars & (close_loc < 0.4) & is_down_bar
                buying_climax = climax_bars & (close_loc > 0.6) & is_up_bar
            else:
                selling_climax = pd.Series(False, index=df.index)
                buying_climax = pd.Series(False, index=df.index)
            
            # 3. No Demand (отсутствие спроса)
            # Узкий спред, низкий объем, цена не может подняться
            no_demand = is_up_bar & is_narrow_range & (relative_volume < 0.8)
            
            # 4. No Supply (отсутствие предложения)
            # Узкий спред, низкий объем, цена не может упасть
            no_supply = is_down_bar & is_narrow_range & (relative_volume < 0.8)
            
            # 5. Test (тест)
            # Падение цены до предыдущего уровня поддержки с низким объемом и последующим закрытием выше
            test = is_down_bar & (relative_volume < 0.8) & (close_loc > 0.5)
            
            # 6. Effort Up (усилие вверх)
            # Растущая цена с увеличивающимся объемом и спредом
            effort_up = is_up_bar & is_wide_range & (relative_volume > self.volume_threshold)
            
            # 7. Effort Down (усилие вниз)
            # Падающая цена с увеличивающимся объемом и спредом
            effort_down = is_down_bar & is_wide_range & (relative_volume > self.volume_threshold)
            
            # Сохраняем результат
            self.result = {
                'relative_volume': relative_volume,
                'close_loc': close_loc,
                'spread': spread,
                'stopping_volume': stopping_volume,
                'selling_climax': selling_climax,
                'buying_climax': buying_climax,
                'no_demand': no_demand,
                'no_supply': no_supply,
                'test': test,
                'effort_up': effort_up,
                'effort_down': effort_down
            }
            
            return self.result
        except Exception as e:
            logger.error(f"Ошибка при расчете VSA: {e}")
            return {}
    
    def generate_signals(self, df: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе VSA
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if self.result is None:
            self.calculate(df)
        
        if not self.result:
            return signals
        
        try:
            # Получаем результаты расчета
            stopping_volume = self.result['stopping_volume']
            selling_climax = self.result['selling_climax']
            buying_climax = self.result['buying_climax']
            no_demand = self.result['no_demand']
            no_supply = self.result['no_supply']
            test = self.result['test']
            effort_up = self.result['effort_up']
            effort_down = self.result['effort_down']
            
            # Генерация сигналов
            for i in range(1, len(df)):
                price = df['close'].iloc[i]
                
                # Сигналы для остановки нисходящего тренда
                if stopping_volume.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.75,
                        indicator=f"{self.name}_StoppingVolume",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: Stopping Volume (потенциальный разворот вниз)",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Сигналы для кульминаций
                if selling_climax.iloc[i]:
                    signal = Signal(
                        type=SignalType.STRONG_BUY,
                        strength=0.9,
                        indicator=f"{self.name}_SellingClimax",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: Selling Climax (вероятный разворот вверх)",
                        stop_loss=price * 0.95,  # Стоп-лосс на 5% ниже
                        take_profit=price * 1.10  # Тейк-профит на 10% выше
                    )
                    
                    signals.append(signal)
                
                if buying_climax.iloc[i]:
                    signal = Signal(
                        type=SignalType.STRONG_SELL,
                        strength=0.9,
                        indicator=f"{self.name}_BuyingClimax",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: Buying Climax (вероятный разворот вниз)",
                        stop_loss=price * 1.05,  # Стоп-лосс на 5% выше
                        take_profit=price * 0.90  # Тейк-профит на 10% ниже
                    )
                    
                    signals.append(signal)
                
                # Сигналы для отсутствия спроса/предложения
                if no_demand.iloc[i]:
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=0.7,
                        indicator=f"{self.name}_NoDemand",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: No Demand (слабость восходящего движения)",
                        stop_loss=price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
                
                if no_supply.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.7,
                        indicator=f"{self.name}_NoSupply",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: No Supply (слабость нисходящего движения)",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Сигналы для тестов
                if test.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.8,
                        indicator=f"{self.name}_Test",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: Test (тест уровня поддержки с низким объемом)",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                # Сигналы для усилий
                if effort_up.iloc[i]:
                    signal = Signal(
                        type=SignalType.BUY,
                        strength=0.75,
                        indicator=f"{self.name}_EffortUp",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: Effort Up (усилие вверх, сильный спрос)",
                        stop_loss=price * 0.97,  # Стоп-лосс на 3% ниже
                        take_profit=price * 1.06  # Тейк-профит на 6% выше
                    )
                    
                    signals.append(signal)
                
                if effort_down.iloc[i]:
                    signal = Signal(
                        type=SignalType.SELL,
                        strength=0.75,
                        indicator=f"{self.name}_EffortDown",
                        timestamp=df.index[i] if isinstance(df.index, pd.DatetimeIndex) else None,
                        timeframe=self.params.get('timeframe', ''),
                        price=price,
                        description=f"VSA: Effort Down (усилие вниз, сильное предложение)",
                        stop_loss=price * 1.03,  # Стоп-лосс на 3% выше
                        take_profit=price * 0.94  # Тейк-профит на 6% ниже
                    )
                    
                    signals.append(signal)
            
            # Сохраняем сигналы
            self.signals = signals
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов VSA: {e}")
            return []

# ===========================================================================
# БЛОК 8: РЕАЛИЗАЦИЯ ДЕТЕКТОРОВ ПАТТЕРНОВ
# ===========================================================================

class PricePatternDetector(BasePatternDetector):
    """
    Детектор ценовых паттернов
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора ценовых паттернов
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.pattern_type = PatternType.PRICE
        
        # Настройка параметров
        self.min_pattern_length = self.params.get('min_pattern_length', 5)  # Минимальная длина паттерна
        self.max_pattern_length = self.params.get('max_pattern_length', 30)  # Максимальная длина паттерна
        self.deviation_threshold = self.params.get('deviation_threshold', 0.03)  # Порог отклонения для идентификации паттернов
        self.head_shoulders_threshold = self.params.get('head_shoulders_threshold', 0.02)  # Порог для идентификации головы и плеч
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для обнаружения ценовых паттернов
        
        Returns:
            List[str]: Список колонок
        """
        return ['high', 'low', 'close']
    
    def detect(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение ценовых паттернов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        if not self.validate_data(df):
            return []
        
        patterns = []
        
        try:
            # Обнаружение паттернов "голова и плечи" и "перевернутые голова и плечи"
            head_shoulders_patterns = self._detect_head_and_shoulders(df)
            patterns.extend(head_shoulders_patterns)
            
            # Обнаружение паттернов "двойное дно" и "двойная вершина"
            double_patterns = self._detect_double_patterns(df)
            patterns.extend(double_patterns)
            
            # Обнаружение паттернов "треугольник"
            triangle_patterns = self._detect_triangles(df)
            patterns.extend(triangle_patterns)
            
            # Обнаружение паттернов "флаг" и "вымпел"
            flag_patterns = self._detect_flags_and_pennants(df)
            patterns.extend(flag_patterns)
            
            # Сохраняем паттерны
            self.patterns = patterns
            
            return patterns
        except Exception as e:
            logger.error(f"Ошибка при обнаружении ценовых паттернов: {e}")
            return []
    
    def _find_local_extrema(self, df: pd.DataFrame, window: int = 5) -> Tuple[pd.Series, pd.Series]:
        """
        Поиск локальных максимумов и минимумов
        
        Args:
            df: DataFrame с данными
            window: Размер окна для поиска
            
        Returns:
            Tuple[pd.Series, pd.Series]: Серии с локальными максимумами и минимумами
        """
        # Инициализация серий для локальных максимумов и минимумов
        peaks = pd.Series(False, index=df.index)
        troughs = pd.Series(False, index=df.index)
        
        # Поиск локальных максимумов и минимумов
        for i in range(window, len(df) - window):
            # Локальный максимум
            if all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, window+1)) and \
               all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, window+1)):
                peaks.iloc[i] = True
            
            # Локальный минимум
            if all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, window+1)) and \
               all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, window+1)):
                troughs.iloc[i] = True
        
        return peaks, troughs
    
    def _detect_head_and_shoulders(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение паттернов "голова и плечи" и "перевернутые голова и плечи"
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        patterns = []
        
        # Поиск локальных максимумов и минимумов
        peaks, troughs = self._find_local_extrema(df)
        
        # Индексы пиков и впадин
        peak_indices = df.index[peaks]
        trough_indices = df.index[troughs]
        
        # Обнаружение паттерна "голова и плечи"
        # Требуется три последовательных пика, где средний пик выше крайних
        for i in range(len(peak_indices) - 2):
            idx1, idx2, idx3 = peak_indices[i], peak_indices[i+1], peak_indices[i+2]
            
            # Проверка паттерна "голова и плечи"
            if df.loc[idx2, 'high'] > df.loc[idx1, 'high'] and df.loc[idx2, 'high'] > df.loc[idx3, 'high'] and \
               abs(df.loc[idx1, 'high'] - df.loc[idx3, 'high']) / df.loc[idx1, 'high'] < self.head_shoulders_threshold:
                
                # Находим и проверяем линию шеи (neckline)
                if len(trough_indices) >= 2:
                    # Ищем ближайшие впадины между пиками
                    trough1 = trough_indices[trough_indices.searchsorted(idx1)]
                    trough2 = trough_indices[trough_indices.searchsorted(idx3)]
                    
                    if trough1 < idx2 and trough2 > idx2:
                        # Расчет линии шеи
                        neckline = (df.loc[trough1, 'low'] + df.loc[trough2, 'low']) / 2
                        
                        # Определение начала и конца паттерна
                        start_idx = df.index.get_loc(idx1) - 5  # 5 баров перед первым пиком
                        end_idx = df.index.get_loc(idx3) + 5    # 5 баров после третьего пика
                        
                        # Ограничиваем индексы размером DataFrame
                        start_idx = max(0, start_idx)
                        end_idx = min(len(df) - 1, end_idx)
                        
                        # Создаем паттерн
                        confidence = 0.8  # Высокая уверенность для четкого паттерна
                        
                        pattern = Pattern(
                            type=PatternType.PRICE,
                            subtype="Head_and_Shoulders",
                            start_idx=start_idx,
                            end_idx=end_idx,
                            confidence=confidence,
                            expected_direction=SignalType.SELL,
                            description="Паттерн 'Голова и плечи' указывает на вероятный разворот вниз",
                            support_levels=[neckline * 0.98],  # Немного ниже линии шеи
                            resistance_levels=[df.loc[idx2, 'high'] * 1.02],  # Немного выше головы
                            price_targets=[neckline - (df.loc[idx2, 'high'] - neckline)]  # Проекция высоты головы вниз от линии шеи
                        )
                        
                        patterns.append(pattern)
        
        # Обнаружение паттерна "перевернутые голова и плечи"
        # Требуется три последовательных впадины, где средняя впадина ниже крайних
        for i in range(len(trough_indices) - 2):
            idx1, idx2, idx3 = trough_indices[i], trough_indices[i+1], trough_indices[i+2]
            
            # Проверка паттерна "перевернутые голова и плечи"
            if df.loc[idx2, 'low'] < df.loc[idx1, 'low'] and df.loc[idx2, 'low'] < df.loc[idx3, 'low'] and \
               abs(df.loc[idx1, 'low'] - df.loc[idx3, 'low']) / df.loc[idx1, 'low'] < self.head_shoulders_threshold:
                
                # Находим и проверяем линию шеи (neckline)
                if len(peak_indices) >= 2:
                    # Ищем ближайшие пики между впадинами
                    peak1 = peak_indices[peak_indices.searchsorted(idx1)]
                    peak2 = peak_indices[peak_indices.searchsorted(idx3)]
                    
                    if peak1 < idx2 and peak2 > idx2:
                        # Расчет линии шеи
                        neckline = (df.loc[peak1, 'high'] + df.loc[peak2, 'high']) / 2
                        
                        # Определение начала и конца паттерна
                        start_idx = df.index.get_loc(idx1) - 5  # 5 баров перед первой впадиной
                        end_idx = df.index.get_loc(idx3) + 5    # 5 баров после третьей впадины
                        
                        # Ограничиваем индексы размером DataFrame
                        start_idx = max(0, start_idx)
                        end_idx = min(len(df) - 1, end_idx)
                        
                        # Создаем паттерн
                        confidence = 0.8  # Высокая уверенность для четкого паттерна
                        
                        pattern = Pattern(
                            type=PatternType.PRICE,
                            subtype="Inverse_Head_and_Shoulders",
                            start_idx=start_idx,
                            end_idx=end_idx,
                            confidence=confidence,
                            expected_direction=SignalType.BUY,
                            description="Паттерн 'Перевернутые голова и плечи' указывает на вероятный разворот вверх",
                            support_levels=[df.loc[idx2, 'low'] * 0.98],  # Немного ниже головы
                            resistance_levels=[neckline * 1.02],  # Немного выше линии шеи
                            price_targets=[neckline + (neckline - df.loc[idx2, 'low'])]  # Проекция высоты головы вверх от линии шеи
                        )
                        
                        patterns.append(pattern)
        
        return patterns
    
    def _detect_double_patterns(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение паттернов "двойное дно" и "двойная вершина"
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        patterns = []
        
        # Поиск локальных максимумов и минимумов
        peaks, troughs = self._find_local_extrema(df)
        
        # Индексы пиков и впадин
        peak_indices = df.index[peaks]
        trough_indices = df.index[troughs]
        
        # Обнаружение паттерна "двойная вершина"
        for i in range(len(peak_indices) - 1):
            idx1, idx2 = peak_indices[i], peak_indices[i+1]
            
            # Проверка наличия впадины между пиками
            mid_trough_indices = trough_indices[(trough_indices > idx1) & (trough_indices < idx2)]
            
            if len(mid_trough_indices) > 0:
                mid_idx = mid_trough_indices[0]
                
                # Проверка примерного равенства высот вершин
                high1 = df.loc[idx1, 'high']
                high2 = df.loc[idx2, 'high']
                
                if abs(high1 - high2) / high1 < self.deviation_threshold:
                    # Расчет линии шеи
                    neckline = df.loc[mid_idx, 'low']
                    
                    # Определение начала и конца паттерна
                    start_idx = df.index.get_loc(idx1) - 5
                    end_idx = df.index.get_loc(idx2) + 5
                    
                    # Ограничиваем индексы размером DataFrame
                    start_idx = max(0, start_idx)
                    end_idx = min(len(df) - 1, end_idx)
                    
                    # Создаем паттерн
                    confidence = 0.75
                    
                    pattern = Pattern(
                        type=PatternType.PRICE,
                        subtype="Double_Top",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=confidence,
                        expected_direction=SignalType.SELL,
                        description="Паттерн 'Двойная вершина' указывает на вероятный разворот вниз",
                        support_levels=[neckline * 0.98],
                        resistance_levels=[high1 * 1.02],
                        price_targets=[neckline - (high1 - neckline)]  # Проекция высоты паттерна вниз от линии шеи
                    )
                    
                    patterns.append(pattern)
        
        # Обнаружение паттерна "двойное дно"
        for i in range(len(trough_indices) - 1):
            idx1, idx2 = trough_indices[i], trough_indices[i+1]
            
            # Проверка наличия пика между впадинами
            mid_peak_indices = peak_indices[(peak_indices > idx1) & (peak_indices < idx2)]
            
            if len(mid_peak_indices) > 0:
                mid_idx = mid_peak_indices[0]
                
                # Проверка примерного равенства глубин впадин
                low1 = df.loc[idx1, 'low']
                low2 = df.loc[idx2, 'low']
                
                if abs(low1 - low2) / low1 < self.deviation_threshold:
                    # Расчет линии шеи
                    neckline = df.loc[mid_idx, 'high']
                    
                    # Определение начала и конца паттерна
                    start_idx = df.index.get_loc(idx1) - 5
                    end_idx = df.index.get_loc(idx2) + 5
                    
                    # Ограничиваем индексы размером DataFrame
                    start_idx = max(0, start_idx)
                    end_idx = min(len(df) - 1, end_idx)
                    
                    # Создаем паттерн
                    confidence = 0.75
                    
                    pattern = Pattern(
                        type=PatternType.PRICE,
                        subtype="Double_Bottom",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=confidence,
                        expected_direction=SignalType.BUY,
                        description="Паттерн 'Двойное дно' указывает на вероятный разворот вверх",
                        support_levels=[low1 * 0.98],
                        resistance_levels=[neckline * 1.02],
                        price_targets=[neckline + (neckline - low1)]  # Проекция высоты паттерна вверх от линии шеи
                    )
                    
                    patterns.append(pattern)
        
        return patterns
    
    def _detect_triangles(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение паттернов "треугольник"
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        patterns = []
        
        # Поиск локальных максимумов и минимумов
        peaks, troughs = self._find_local_extrema(df, window=3)
        
        # Индексы пиков и впадин
        peak_indices = list(df.index[peaks])
        trough_indices = list(df.index[troughs])
        
        # Требуется минимум 3 пика и 3 впадины для идентификации треугольника
        if len(peak_indices) >= 3 and len(trough_indices) >= 3:
            # Симметричный треугольник: нисходящие максимумы и восходящие минимумы
            # Восходящий треугольник: горизонтальные сопротивления и восходящие поддержки
            # Нисходящий треугольник: нисходящие сопротивления и горизонтальные поддержки
            
            # Проверка последних нескольких экстремумов
            last_peaks = peak_indices[-3:]
            last_troughs = trough_indices[-3:]
            
            # Сортировка по времени
            last_peaks.sort()
            last_troughs.sort()
            
            # Получение значений
            peak_values = [df.loc[idx, 'high'] for idx in last_peaks]
            trough_values = [df.loc[idx, 'low'] for idx in last_troughs]
            
            # Определение наклона линий
            peak_slope = (peak_values[2] - peak_values[0]) / (df.index.get_loc(last_peaks[2]) - df.index.get_loc(last_peaks[0]))
            trough_slope = (trough_values[2] - trough_values[0]) / (df.index.get_loc(last_troughs[2]) - df.index.get_loc(last_troughs[0]))
            
            # Определение типа треугольника на основе наклона линий
            triangle_type = None
            expected_direction = None
            
            # Симметричный треугольник
            if peak_slope < -0.0001 and trough_slope > 0.0001:
                triangle_type = "Symmetrical_Triangle"
                # Направление определяется по предшествующему тренду
                # Здесь упрощенно: если последняя цена ближе к верхней линии - вверх, иначе - вниз
                last_price = df['close'].iloc[-1]
                upper_line = peak_values[0] + peak_slope * (len(df) - df.index.get_loc(last_peaks[0]))
                lower_line = trough_values[0] + trough_slope * (len(df) - df.index.get_loc(last_troughs[0]))
                mid_line = (upper_line + lower_line) / 2
                
                expected_direction = SignalType.BUY if last_price > mid_line else SignalType.SELL
            
            # Восходящий треугольник
            elif abs(peak_slope) < 0.0001 and trough_slope > 0.0001:
                triangle_type = "Ascending_Triangle"
                expected_direction = SignalType.BUY
            
            # Нисходящий треугольник
            elif peak_slope < -0.0001 and abs(trough_slope) < 0.0001:
                triangle_type = "Descending_Triangle"
                expected_direction = SignalType.SELL
            
            # Если определен тип треугольника, создаем паттерн
            if triangle_type:
                # Определение начала и конца паттерна
                start_idx = min(df.index.get_loc(last_peaks[0]), df.index.get_loc(last_troughs[0])) - 5
                end_idx = max(df.index.get_loc(last_peaks[2]), df.index.get_loc(last_troughs[2])) + 5
                
                # Ограничиваем индексы размером DataFrame
                start_idx = max(0, start_idx)
                end_idx = min(len(df) - 1, end_idx)
                
                # Расчет точки пробоя (сходимости линий)
                # x_convergence = (trough_values[0] - peak_values[0]) / (peak_slope - trough_slope)
                # convergence_idx = int(df.index.get_loc(last_peaks[0]) + x_convergence)
                # convergence_idx = min(len(df) - 1, max(0, convergence_idx))
                
                # Создаем паттерн
                confidence = 0.7
                
                # Описание и цели зависят от типа треугольника
                if triangle_type == "Symmetrical_Triangle":
                    description = "Симметричный треугольник - консолидация перед продолжением тренда"
                    price_targets = [df['close'].iloc[-1] * 1.05] if expected_direction == SignalType.BUY else [df['close'].iloc[-1] * 0.95]
                elif triangle_type == "Ascending_Triangle":
                    description = "Восходящий треугольник - бычий паттерн, ожидается пробой вверх"
                    price_targets = [peak_values[0] * 1.05]  # Цель 5% выше уровня сопротивления
                else:  # Descending_Triangle
                    description = "Нисходящий треугольник - медвежий паттерн, ожидается пробой вниз"
                    price_targets = [trough_values[0] * 0.95]  # Цель 5% ниже уровня поддержки
                
                pattern = Pattern(
                    type=PatternType.PRICE,
                    subtype=triangle_type,
                    start_idx=start_idx,
                    end_idx=end_idx,
                    confidence=confidence,
                    expected_direction=expected_direction,
                    description=description,
                    support_levels=[trough_values[-1]],
                    resistance_levels=[peak_values[-1]],
                    price_targets=price_targets
                )
                
                patterns.append(pattern)
        
        return patterns
    
    def _detect_flags_and_pennants(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение паттернов "флаг" и "вымпел"
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        patterns = []
        
        # Минимальная длина для флага/вымпела
        min_length = 5
        max_length = 20
        
        # Проверка наличия тренда перед паттерном
        # Скользящее окно для проверки последовательных сегментов данных
        for i in range(len(df) - max_length - min_length):
            # Проверка наличия сильного движения (флагшток)
            pole_start = i
            pole_end = i + min_length
            
            pole_move = df['close'].iloc[pole_end] - df['close'].iloc[pole_start]
            
            # Если есть сильное движение
            if abs(pole_move) / df['close'].iloc[pole_start] > 0.05:  # Минимум 5% движение
                # Проверка флага/вымпела после движения
                flag_start = pole_end
                flag_end = flag_start + min_length
                
                # Проверка, что флаг не слишком большой относительно флагштока
                flag_move = df['close'].iloc[flag_end] - df['close'].iloc[flag_start]
                
                if abs(flag_move) < abs(pole_move) * 0.5:  # Флаг меньше 50% от флагштока
                    # Определение типа паттерна
                    is_bullish = pole_move > 0
                    
                    # Для бычьего флага/вымпела: флагшток вверх, флаг - небольшая коррекция вниз
                    # Для медвежьего флага/вымпела: флагшток вниз, флаг - небольшая коррекция вверх
                    correct_direction = (is_bullish and flag_move < 0) or (not is_bullish and flag_move > 0)
                    
                    if correct_direction:
                        # Определение типа: флаг или вымпел
                        # Флаг: параллельные линии
                        # Вымпел: сходящиеся линии (треугольник)
                        
                        # Расчет верхней и нижней границы
                        flag_highs = df['high'].iloc[flag_start:flag_end+1]
                        flag_lows = df['low'].iloc[flag_start:flag_end+1]
                        
                        # Линейная регрессия для верхней и нижней границы
                        if SCIPY_AVAILABLE:
                            try:
                                x = np.arange(len(flag_highs))
                                
                                # Регрессия для верхней границы
                                slope_high, intercept_high, _, _, _ = stats.linregress(x, flag_highs)
                                
                                # Регрессия для нижней границы
                                slope_low, intercept_low, _, _, _ = stats.linregress(x, flag_lows)
                                
                                # Определение типа на основе наклона линий
                                is_pennant = (is_bullish and slope_high < 0 and slope_low > 0) or \
                                             (not is_bullish and slope_high > 0 and slope_low < 0)
                                
                                pattern_type = "Pennant" if is_pennant else "Flag"
                                
                                # Создаем паттерн
                                subtype = f"{'Bullish' if is_bullish else 'Bearish'}_{pattern_type}"
                                expected_direction = SignalType.BUY if is_bullish else SignalType.SELL
                                
                                # Расчет уровней поддержки и сопротивления
                                support = flag_lows.iloc[-1]
                                resistance = flag_highs.iloc[-1]
                                
                                # Расчет целевой цены (длина флагштока от точки пробоя)
                                price_target = None
                                if is_bullish:
                                    price_target = df['close'].iloc[flag_end] + abs(pole_move)
                                else:
                                    price_target = df['close'].iloc[flag_end] - abs(pole_move)
                                
                                pattern = Pattern(
                                    type=PatternType.PRICE,
                                    subtype=subtype,
                                    start_idx=pole_start,
                                    end_idx=flag_end,
                                    confidence=0.75,
                                    expected_direction=expected_direction,
                                    description=f"{subtype} указывает на вероятное продолжение {'восходящего' if is_bullish else 'нисходящего'} тренда",
                                    support_levels=[support],
                                    resistance_levels=[resistance],
                                    price_targets=[price_target]
                                )
                                
                                patterns.append(pattern)
                            except:
                                # Пропускаем, если не удалось сделать регрессию
                                pass
        
        return patterns

class VolumePatternDetector(BasePatternDetector):
    """
    Детектор объемных паттернов
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора объемных паттернов
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.pattern_type = PatternType.VOLUME
        
        # Настройка параметров
        self.volume_threshold = self.params.get('volume_threshold', 2.0)  # Порог для высокого объема (x от среднего)
        self.lookback_period = self.params.get('lookback_period', 20)  # Период для анализа
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для обнаружения объемных паттернов
        
        Returns:
            List[str]: Список колонок
        """
        return ['open', 'high', 'low', 'close', 'volume']
    
    def detect(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение объемных паттернов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        if not self.validate_data(df):
            return []
        
        patterns = []
        
        try:
            # Расчет скользящего среднего объема
            volume_ma = df['volume'].rolling(window=self.lookback_period).mean()
            
            # Расчет относительного объема
            relative_volume = df['volume'] / volume_ma
            
            # Расчет размера свечи
            candle_range = df['high'] - df['low']
            candle_body = abs(df['close'] - df['open'])
            
            # Обнаружение паттернов
            
            # 1. Volume Climax (объемная кульминация)
            volume_climax = relative_volume > self.volume_threshold
            
            # 2. Volume Exhaustion (истощение объема)
            # Сначала находим локальные максимумы объема
            volume_peaks = (df['volume'] > df['volume'].shift()) & (df['volume'] > df['volume'].shift(-1))
            # Затем находим убывающие последовательности максимумов
            decreasing_peaks = []
            for i in range(len(df) - 5):
                if volume_peaks.iloc[i] and volume_peaks.iloc[i+5]:
                    if df['volume'].iloc[i] > df['volume'].iloc[i+5] and df['close'].iloc[i] < df['close'].iloc[i+5]:
                        decreasing_peaks.append(i)
            
            # 3. Churn (перетирание)
            # Высокий объем с маленьким диапазоном свечи
            churn = (relative_volume > 1.5) & (candle_range < candle_range.rolling(window=10).mean() * 0.7)
            
            # 4. Ultra-high Volume (аномально высокий объем)
            ultra_high_volume = relative_volume > self.volume_threshold * 1.5
            
            # Генерация паттернов
            
            # Volume Climax
            for i in range(1, len(df) - 1):
                if volume_climax.iloc[i]:
                    # Определение направления
                    is_bullish = df['close'].iloc[i] > df['open'].iloc[i]
                    expected_direction = SignalType.BUY if is_bullish else SignalType.SELL
                    
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 3)
                    end_idx = min(len(df) - 1, i + 3)
                    
                    # Определение уровней поддержки и сопротивления
                    support = df['low'].iloc[i] * 0.98
                    resistance = df['high'].iloc[i] * 1.02
                    
                    # Определение целевой цены
                    if is_bullish:
                        price_target = df['close'].iloc[i] * 1.05  # 5% выше текущей цены
                    else:
                        price_target = df['close'].iloc[i] * 0.95  # 5% ниже текущей цены
                    
                    # Создаем паттерн
                    pattern = Pattern(
                        type=PatternType.VOLUME,
                        subtype="Volume_Climax",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.8,
                        expected_direction=expected_direction,
                        description=f"{'Бычья' if is_bullish else 'Медвежья'} объемная кульминация: аномально высокий объем ({relative_volume.iloc[i]:.2f}x) при {'растущей' if is_bullish else 'падающей'} цене",
                        support_levels=[support],
                        resistance_levels=[resistance],
                        price_targets=[price_target]
                    )
                    
                    patterns.append(pattern)
            
            # Volume Exhaustion
            for i in decreasing_peaks:
                if i < len(df) - 5:
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 2)
                    end_idx = min(len(df) - 1, i + 5)
                    
                    # Определение целевой цены (продолжение текущего тренда)
                    is_uptrend = df['close'].iloc[i+5] > df['close'].iloc[i]
                    expected_direction = SignalType.BUY if is_uptrend else SignalType.SELL
                    
                    # Определение уровней поддержки и сопротивления
                    support = df['low'].iloc[i:i+5].min() * 0.98
                    resistance = df['high'].iloc[i:i+5].max() * 1.02
                    
                    # Определение целевой цены
                    if is_uptrend:
                        price_target = df['close'].iloc[i+5] * 1.05  # 5% выше текущей цены
                    else:
                        price_target = df['close'].iloc[i+5] * 0.95  # 5% ниже текущей цены
                    
                    # Создаем паттерн
                    pattern = Pattern(
                        type=PatternType.VOLUME,
                        subtype="Volume_Exhaustion",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.7,
                        expected_direction=expected_direction,
                        description=f"Истощение объема: снижение объемов при {'растущем' if is_uptrend else 'падающем'} тренде, указывает на {'продолжение' if is_uptrend else 'разворот'} тренда",
                        support_levels=[support],
                        resistance_levels=[resistance],
                        price_targets=[price_target]
                    )
                    
                    patterns.append(pattern)
            
            # Churn (перетирание)
            for i in range(1, len(df) - 1):
                if churn.iloc[i]:
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 2)
                    end_idx = min(len(df) - 1, i + 2)
                    
                    # Определение направления предыдущего тренда
                    prev_trend = df['close'].iloc[i-1] > df['close'].iloc[max(0, i-5)]
                    
                    # Ожидаемое направление (против предыдущего тренда)
                    expected_direction = SignalType.SELL if prev_trend else SignalType.BUY
                    
                    # Определение уровней поддержки и сопротивления
                    support = df['low'].iloc[i] * 0.98
                    resistance = df['high'].iloc[i] * 1.02
                    
                    # Определение целевой цены
                    if expected_direction == SignalType.BUY:
                        price_target = df['close'].iloc[i] * 1.03  # 3% выше текущей цены
                    else:
                        price_target = df['close'].iloc[i] * 0.97  # 3% ниже текущей цены
                    
                    # Создаем паттерн
                    pattern = Pattern(
                        type=PatternType.VOLUME,
                        subtype="Churn",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.6,
                        expected_direction=expected_direction,
                        description=f"Объемное перетирание: высокий объем при низкой волатильности, возможная смена направления тренда",
                        support_levels=[support],
                        resistance_levels=[resistance],
                        price_targets=[price_target]
                    )
                    
                    patterns.append(pattern)
            
            # Ultra-high Volume (аномально высокий объем)
            for i in range(1, len(df) - 1):
                if ultra_high_volume.iloc[i]:
                    # Определение направления
                    is_bullish = df['close'].iloc[i] > df['open'].iloc[i]
                    expected_direction = SignalType.BUY if is_bullish else SignalType.SELL
                    
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 3)
                    end_idx = min(len(df) - 1, i + 5)
                    
                    # Определение уровней поддержки и сопротивления
                    support = df['low'].iloc[i] * 0.97
                    resistance = df['high'].iloc[i] * 1.03
                    
                    # Определение целевой цены
                    if is_bullish:
                        price_target = df['close'].iloc[i] * 1.07  # 7% выше текущей цены
                    else:
                        price_target = df['close'].iloc[i] * 0.93  # 7% ниже текущей цены
                    
                    # Создаем паттерн
                    pattern = Pattern(
                        type=PatternType.VOLUME,
                        subtype="Ultra_High_Volume",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.85,
                        expected_direction=expected_direction,
                        description=f"Аномально высокий объем ({relative_volume.iloc[i]:.2f}x от среднего): возможен сильный импульс {'вверх' if is_bullish else 'вниз'}",
                        support_levels=[support],
                        resistance_levels=[resistance],
                        price_targets=[price_target]
                    )
                    
                    patterns.append(pattern)
            
            # Сохраняем паттерны
            self.patterns = patterns
            
            return patterns
        except Exception as e:
            logger.error(f"Ошибка при обнаружении объемных паттернов: {e}")
            return []

class VolatilityPatternDetector(BasePatternDetector):
    """
    Детектор паттернов волатильности
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора паттернов волатильности
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.pattern_type = PatternType.VOLATILITY
        
        # Настройка параметров
        self.lookback_period = self.params.get('lookback_period', 20)  # Период для анализа
        self.volatility_threshold = self.params.get('volatility_threshold', 2.0)  # Порог для высокой волатильности
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для обнаружения паттернов волатильности
        
        Returns:
            List[str]: Список колонок
        """
        return ['high', 'low', 'close']
    
    def detect(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение паттернов волатильности
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        if not self.validate_data(df):
            return []
        
        patterns = []
        
        try:
            # Расчет волатильности
            # Используем True Range как меру волатильности
            high_low = df['high'] - df['low']
            high_close = (df['high'] - df['close'].shift()).abs()
            low_close = (df['low'] - df['close'].shift()).abs()
            
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = tr.rolling(window=self.lookback_period).mean()
            
            # Отношение текущего TR к ATR
            tr_ratio = tr / atr
            
            # Расчет сжатия волатильности (Compression)
            # Сжатие: ATR падает и ниже среднего
            atr_ma = atr.rolling(window=50).mean()
            atr_decreasing = atr < atr.shift(5)
            atr_low = atr < atr_ma * 0.8
            
            compression = atr_decreasing & atr_low
            
            # Обнаружение резкого увеличения волатильности (Expansion)
            # Резкое увеличение: TR > ATR * threshold
            expansion = tr_ratio > self.volatility_threshold
            
            # Обнаружение Volatility Breakout (пробой после сжатия)
            expansion_after_compression = expansion & compression.shift(1)
            
            # Генерация паттернов
            
            # Сжатие волатильности (Compression)
            for i in range(5, len(df) - 1):
                if compression.iloc[i] and not compression.iloc[i-1]:
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 5)
                    end_idx = i
                    
                    # Определение ценового диапазона
                    price_range = (df['high'].iloc[start_idx:end_idx+1].max() - df['low'].iloc[start_idx:end_idx+1].min())
                    
                    # Создаем паттерн
                    pattern = Pattern(
                        type=PatternType.VOLATILITY,
                        subtype="Volatility_Compression",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.7,
                        expected_direction=SignalType.UNDEFINED,  # Еще не известно направление
                        description="Сжатие волатильности: возможен сильный импульс в ближайшее время",
                        support_levels=[df['low'].iloc[end_idx] * 0.99],
                        resistance_levels=[df['high'].iloc[end_idx] * 1.01],
                        price_targets=[
                            df['close'].iloc[end_idx] + price_range,  # Цель вверх
                            df['close'].iloc[end_idx] - price_range   # Цель вниз
                        ]
                    )
                    
                    patterns.append(pattern)
            
            # Резкое увеличение волатильности (Expansion)
            for i in range(1, len(df) - 1):
                if expansion.iloc[i]:
                    # Определение направления
                    is_bullish = df['close'].iloc[i] > df['open'].iloc[i]
                    expected_direction = SignalType.BUY if is_bullish else SignalType.SELL
                    
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 1)
                    end_idx = i
                    
                    # Определение процента изменения
                    percent_change = abs(df['close'].iloc[i] / df['close'].iloc[i-1] - 1) * 100
                    
                    # Создаем паттерн
                    pattern = Pattern(
                        type=PatternType.VOLATILITY,
                        subtype="Volatility_Expansion",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.75,
                        expected_direction=expected_direction,
                        description=f"Резкое увеличение волатильности: {percent_change:.2f}% изменение за одну свечу",
                        support_levels=[df['low'].iloc[i] * 0.98],
                        resistance_levels=[df['high'].iloc[i] * 1.02],
                        price_targets=[
                            df['close'].iloc[i] * 1.05 if is_bullish else df['close'].iloc[i] * 0.95
                        ]
                    )
                    
                    patterns.append(pattern)
            
            # Пробой после сжатия (Volatility Breakout)
            for i in range(5, len(df) - 1):
                if expansion_after_compression.iloc[i]:
                    # Определение направления
                    is_bullish = df['close'].iloc[i] > df['open'].iloc[i]
                    expected_direction = SignalType.BUY if is_bullish else SignalType.SELL
                    
                    # Определение начала и конца паттерна
                    start_idx = max(0, i - 10)
                    end_idx = i
                    
                    # Определение ценового диапазона до пробоя
                    pre_breakout_range = (df['high'].iloc[start_idx:i].max() - df['low'].iloc[start_idx:i].min())
                    
                    # Создаем паттерн с высокой уверенностью
                    pattern = Pattern(
                        type=PatternType.VOLATILITY,
                        subtype="Volatility_Breakout",
                        start_idx=start_idx,
                        end_idx=end_idx,
                        confidence=0.9,
                        expected_direction=expected_direction,
                        description=f"Пробой после сжатия волатильности: сильное {'бычье' if is_bullish else 'медвежье'} движение",
                        support_levels=[df['low'].iloc[i] * 0.97 if is_bullish else df['low'].iloc[i] * 0.90],
                        resistance_levels=[df['high'].iloc[i] * 1.10 if is_bullish else df['high'].iloc[i] * 1.03],
                        price_targets=[
                            df['close'].iloc[i] + (pre_breakout_range * 1.5) if is_bullish else df['close'].iloc[i] - (pre_breakout_range * 1.5)
                        ]
                    )
                    
                    patterns.append(pattern)
            
            # Сохраняем паттерны
            self.patterns = patterns
            
            return patterns
        except Exception as e:
            logger.error(f"Ошибка при обнаружении паттернов волатильности: {e}")
            return []

class CombinedPatternDetector(BasePatternDetector):
    """
    Детектор комбинированных паттернов
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора комбинированных паттернов
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.pattern_type = PatternType.COMBINED
        
        # Создание детекторов отдельных типов паттернов
        self.price_detector = PricePatternDetector(params)
        self.volume_detector = VolumePatternDetector(params)
        self.volatility_detector = VolatilityPatternDetector(params)
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для обнаружения комбинированных паттернов
        
        Returns:
            List[str]: Список колонок
        """
        # Объединяем все необходимые колонки из отдельных детекторов
        columns = set()
        columns.update(self.price_detector.required_columns)
        columns.update(self.volume_detector.required_columns)
        columns.update(self.volatility_detector.required_columns)
        
        return list(columns)
    
    def detect(self, df: pd.DataFrame) -> List[Pattern]:
        """
        Обнаружение комбинированных паттернов
        
        Args:
            df: DataFrame с данными
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        if not self.validate_data(df):
            return []
        
        try:
            # Получаем паттерны от отдельных детекторов
            price_patterns = self.price_detector.detect(df)
            volume_patterns = self.volume_detector.detect(df)
            volatility_patterns = self.volatility_detector.detect(df)
            
            # Объединяем все паттерны
            all_patterns = price_patterns + volume_patterns + volatility_patterns
            
            # Ищем комбинированные паттерны
            combined_patterns = self._find_combinations(df, all_patterns)
            
            # Возвращаем все паттерны
            result = all_patterns + combined_patterns
            
            # Сохраняем паттерны
            self.patterns = result
            
            return result
        except Exception as e:
            logger.error(f"Ошибка при обнаружении комбинированных паттернов: {e}")
            return []
    
    def _find_combinations(self, df: pd.DataFrame, patterns: List[Pattern]) -> List[Pattern]:
        """
        Поиск комбинаций паттернов
        
        Args:
            df: DataFrame с данными
            patterns: Список обнаруженных паттернов
            
        Returns:
            List[Pattern]: Список комбинированных паттернов
        """
        combined_patterns = []
        
        # Сортируем паттерны по концу
        patterns.sort(key=lambda p: p.end_idx)
        
        # Ищем пересечения паттернов
        for i in range(len(patterns)):
            for j in range(i + 1, len(patterns)):
                pattern1 = patterns[i]
                pattern2 = patterns[j]
                
                # Проверяем пересечение по времени
                if pattern1.end_idx >= pattern2.start_idx:
                    # Проверяем согласованность направления
                    same_direction = pattern1.expected_direction == pattern2.expected_direction
                    
                    if same_direction and pattern1.expected_direction != SignalType.UNDEFINED:
                        # Определение начала и конца комбинированного паттерна
                        start_idx = min(pattern1.start_idx, pattern2.start_idx)
                        end_idx = max(pattern1.end_idx, pattern2.end_idx)
                        
                        # Расчет уверенности
                        confidence = (pattern1.confidence + pattern2.confidence) / 2 + 0.1  # Бонус за комбинацию
                        confidence = min(confidence, 1.0)  # Ограничиваем до 1.0
                        
                        # Определение уровней поддержки и сопротивления
                        support_levels = list(set(pattern1.support_levels + pattern2.support_levels))
                        resistance_levels = list(set(pattern1.resistance_levels + pattern2.resistance_levels))
                        
                        # Определение целевых цен
                        price_targets = list(set(pattern1.price_targets + pattern2.price_targets))
                        
                        # Создаем комбинированный паттерн
                        combined_pattern = Pattern(
                            type=PatternType.COMBINED,
                            subtype=f"{pattern1.subtype}+{pattern2.subtype}",
                            start_idx=start_idx,
                            end_idx=end_idx,
                            confidence=confidence,
                            expected_direction=pattern1.expected_direction,
                            description=f"Комбинированный паттерн: {pattern1.subtype} + {pattern2.subtype} (подтверждение {pattern1.expected_direction.value})",
                            support_levels=support_levels,
                            resistance_levels=resistance_levels,
                            price_targets=price_targets
                        )
                        
                        combined_patterns.append(combined_pattern)
        
        return combined_patterns

class OrderbookPatternDetector(BasePatternDetector):
    """
    Детектор паттернов ордербука
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора паттернов ордербука
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.pattern_type = PatternType.ORDERBOOK
        
        # Настройка параметров
        self.volume_threshold = self.params.get('volume_threshold', 2.0)  # Относительный порог объема для "стены"
        self.imbalance_threshold = self.params.get('imbalance_threshold', 3.0)  # Порог для дисбаланса бид/аск
    
    @property
    def required_columns(self) -> List[str]:
        """
        Список колонок, необходимых для обнаружения паттернов ордербука
        
        Returns:
            List[str]: Список колонок
        """
        return ['close']  # Минимальный набор колонок для основного DataFrame
    
    def detect(self, df: pd.DataFrame, orderbook_data: Optional[Dict] = None) -> List[Pattern]:
        """
        Обнаружение паттернов ордербука
        
        Args:
            df: DataFrame с ценовыми данными
            orderbook_data: Данные ордербука
            
        Returns:
            List[Pattern]: Список обнаруженных паттернов
        """
        if not self.validate_data(df) or orderbook_data is None:
            return []
        
        patterns = []
        
        try:
            # Получаем текущую цену и индекс
            current_price = df['close'].iloc[-1]
            current_idx = len(df) - 1
            
            # Извлекаем бид и аск из ордербука
            bids = orderbook_data.get('bids', [])
            asks = orderbook_data.get('asks', [])
            
            if not bids or not asks:
                return []
            
            # Преобразуем в массивы numpy для быстрой обработки
            bid_prices = np.array([float(bid[0]) for bid in bids])
            bid_volumes = np.array([float(bid[1]) for bid in bids])
            ask_prices = np.array([float(ask[0]) for ask in asks])
            ask_volumes = np.array([float(ask[1]) for ask in asks])
            
            # Вычисляем общий объем
            total_bid_volume = np.sum(bid_volumes)
            total_ask_volume = np.sum(ask_volumes)
            
            # Вычисляем средний объем для выявления "стен"
            avg_bid_volume = np.mean(bid_volumes)
            avg_ask_volume = np.mean(ask_volumes)
            
            # Идентификация "стен" ордеров
            bid_walls = np.where(bid_volumes > avg_bid_volume * self.volume_threshold)[0]
            ask_walls = np.where(ask_volumes > avg_ask_volume * self.volume_threshold)[0]
            
            # Проверка наличия дисбаланса бид/аск
            bid_ask_ratio = total_bid_volume / total_ask_volume if total_ask_volume > 0 else float('inf')
            ask_bid_ratio = total_ask_volume / total_bid_volume if total_bid_volume > 0 else float('inf')
            
            has_bid_imbalance = bid_ask_ratio > self.imbalance_threshold
            has_ask_imbalance = ask_bid_ratio > self.imbalance_threshold
            
            # Генерация паттернов
            
            # Стены ордеров на покупку (поддержка)
            for idx in bid_walls:
                # Определение силы стены (отношение к среднему)
                wall_strength = bid_volumes[idx] / avg_bid_volume
                
                # Определение уверенности на основе силы стены
                confidence = min(0.5 + wall_strength / 10, 0.9)
                
                # Создаем паттерн
                pattern = Pattern(
                    type=PatternType.ORDERBOOK,
                    subtype="Bid_Wall",
                    start_idx=max(0, current_idx - 5),
                    end_idx=current_idx,
                    confidence=confidence,
                    expected_direction=SignalType.BUY,
                    description=f"Стена ордеров на покупку: {wall_strength:.2f}x больше среднего объема на уровне {bid_prices[idx]:.8f}",
                    support_levels=[bid_prices[idx] * 0.99],
                    resistance_levels=[],
                    price_targets=[current_price * 1.02]  # 2% выше текущей цены
                )
                
                patterns.append(pattern)
            
            # Стены ордеров на продажу (сопротивление)
            for idx in ask_walls:
                # Определение силы стены (отношение к среднему)
                wall_strength = ask_volumes[idx] / avg_ask_volume
                
                # Определение уверенности на основе силы стены
                confidence = min(0.5 + wall_strength / 10, 0.9)
                
                # Создаем паттерн
                pattern = Pattern(
                    type=PatternType.ORDERBOOK,
                    subtype="Ask_Wall",
                    start_idx=max(0, current_idx - 5),
                    end_idx=current_idx,
                    confidence=confidence,
                    expected_direction=SignalType.SELL,
                    description=f"Стена ордеров на продажу: {wall_strength:.2f}x больше среднего объема на уровне {ask_prices[idx]:.8f}",
                    support_levels=[],
                    resistance_levels=[ask_prices[idx] * 1.01],
                    price_targets=[current_price * 0.98]  # 2% ниже текущей цены
                )
                
                patterns.append(pattern)
            
            # Дисбаланс бид/аск
            if has_bid_imbalance:
                # Создаем паттерн
                pattern = Pattern(
                    type=PatternType.ORDERBOOK,
                    subtype="Bid_Imbalance",
                    start_idx=max(0, current_idx - 5),
                    end_idx=current_idx,
                    confidence=0.7,
                    expected_direction=SignalType.BUY,
                    description=f"Дисбаланс ордеров: {bid_ask_ratio:.2f}x больше покупок чем продаж",
                    support_levels=[current_price * 0.98],
                    resistance_levels=[],
                    price_targets=[current_price * 1.03]  # 3% выше текущей цены
                )
                
                patterns.append(pattern)
            
            if has_ask_imbalance:
                # Создаем паттерн
                pattern = Pattern(
                    type=PatternType.ORDERBOOK,
                    subtype="Ask_Imbalance",
                    start_idx=max(0, current_idx - 5),
                    end_idx=current_idx,
                    confidence=0.7,
                    expected_direction=SignalType.SELL,
                    description=f"Дисбаланс ордеров: {ask_bid_ratio:.2f}x больше продаж чем покупок",
                    support_levels=[],
                    resistance_levels=[current_price * 1.02],
                    price_targets=[current_price * 0.97]  # 3% ниже текущей цены
                )
                
                patterns.append(pattern)
            
            # Сохраняем паттерны
            self.patterns = patterns
            
            return patterns
        except Exception as e:
            logger.error(f"Ошибка при обнаружении паттернов ордербука: {e}")
            return []

# ===========================================================================
# БЛОК 9: РЕАЛИЗАЦИЯ ДЕТЕКТОРОВ ДИВЕРГЕНЦИЙ
# ===========================================================================

class RSIDivergenceDetector(BaseDivergenceDetector):
    """
    Детектор дивергенций RSI
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора дивергенций RSI
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.name = 'RSIDivergence'
        
        # Настройка параметров
        self.window = self.params.get('window', 5)  # Окно для поиска экстремумов
        self.min_divergence_points = self.params.get('min_divergence_points', 2)  # Минимальное количество точек для дивергенции
        self.min_threshold = self.params.get('min_threshold', 0.03)  # Минимальный порог для дивергенции (3%)
    
    @property
    def required_indicators(self) -> List[str]:
        """
        Список индикаторов, необходимых для обнаружения дивергенций
        
        Returns:
            List[str]: Список индикаторов
        """
        return ['RSI']
    
    def detect(self, df: pd.DataFrame, indicator_data: Dict[str, pd.Series]) -> List[Divergence]:
        """
        Обнаружение дивергенций между ценой и RSI
        
        Args:
            df: DataFrame с данными
            indicator_data: Словарь с данными индикаторов
            
        Returns:
            List[Divergence]: Список обнаруженных дивергенций
        """
        if not self.validate_data(df, indicator_data):
            return []
        
        divergences = []
        
        try:
            # Получаем цену и индикатор
            price = df['close']
            
            # Получаем RSI (может быть вложен в индикаторе)
            rsi = None
            if 'RSI' in indicator_data:
                if isinstance(indicator_data['RSI'], dict) and 'rsi' in indicator_data['RSI']:
                    rsi = indicator_data['RSI']['rsi']
                else:
                    rsi = indicator_data['RSI']
            
            if rsi is None or len(rsi) != len(price):
                logger.warning(f"{self.name}: RSI не найден или имеет неверную длину")
                return []
            
            # Поиск локальных экстремумов для цены
            price_peaks = []
            price_troughs = []
            
            for i in range(self.window, len(price) - self.window):
                # Локальный максимум
                if all(price.iloc[i] > price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] > price.iloc[i+j] for j in range(1, self.window+1)):
                    price_peaks.append(i)
                
                # Локальный минимум
                if all(price.iloc[i] < price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] < price.iloc[i+j] for j in range(1, self.window+1)):
                    price_troughs.append(i)
            
            # Поиск локальных экстремумов для RSI
            rsi_peaks = []
            rsi_troughs = []
            
            for i in range(self.window, len(rsi) - self.window):
                # Локальный максимум
                if all(rsi.iloc[i] > rsi.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(rsi.iloc[i] > rsi.iloc[i+j] for j in range(1, self.window+1)):
                    rsi_peaks.append(i)
                
                # Локальный минимум
                if all(rsi.iloc[i] < rsi.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(rsi.iloc[i] < rsi.iloc[i+j] for j in range(1, self.window+1)):
                    rsi_troughs.append(i)
            
            # Поиск регулярных бычьих дивергенций
            # Цена: более низкие минимумы, RSI: более высокие минимумы
            for i in range(len(price_troughs) - 1):
                for j in range(len(rsi_troughs) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_troughs[i] - rsi_troughs[j]) <= self.window and abs(price_troughs[i+1] - rsi_troughs[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_troughs[i+1]] < price.iloc[price_troughs[i]] and rsi.iloc[rsi_troughs[j+1]] > rsi.iloc[rsi_troughs[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_troughs[i+1]] - price.iloc[price_troughs[i]]) / price.iloc[price_troughs[i]]
                            rsi_change = (rsi.iloc[rsi_troughs[j+1]] - rsi.iloc[rsi_troughs[j]]) / rsi.iloc[rsi_troughs[j]]
                            
                            if abs(price_change) > self.min_threshold and abs(rsi_change) > self.min_threshold:
                                # Определяем силу дивергенции
                                strength = min(0.5 + (abs(price_change) + abs(rsi_change)) / 2, 1.0)
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BULLISH,
                                    price_point1=price_troughs[i],
                                    price_point2=price_troughs[i+1],
                                    indicator_point1=rsi_troughs[j],
                                    indicator_point2=rsi_troughs[j+1],
                                    indicator_name='RSI',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная бычья дивергенция: цена формирует более низкие минимумы, RSI формирует более высокие минимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Поиск регулярных медвежьих дивергенций
            # Цена: более высокие максимумы, RSI: более низкие максимумы
            for i in range(len(price_peaks) - 1):
                for j in range(len(rsi_peaks) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_peaks[i] - rsi_peaks[j]) <= self.window and abs(price_peaks[i+1] - rsi_peaks[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_peaks[i+1]] > price.iloc[price_peaks[i]] and rsi.iloc[rsi_peaks[j+1]] < rsi.iloc[rsi_peaks[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_peaks[i+1]] - price.iloc[price_peaks[i]]) / price.iloc[price_peaks[i]]
                            rsi_change = (rsi.iloc[rsi_peaks[j+1]] - rsi.iloc[rsi_peaks[j]]) / rsi.iloc[rsi_peaks[j]]
                            
                            if abs(price_change) > self.min_threshold and abs(rsi_change) > self.min_threshold:
                                # Определяем силу дивергенции
                                strength = min(0.5 + (abs(price_change) + abs(rsi_change)) / 2, 1.0)
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BEARISH,
                                    price_point1=price_peaks[i],
                                    price_point2=price_peaks[i+1],
                                    indicator_point1=rsi_peaks[j],
                                    indicator_point2=rsi_peaks[j+1],
                                    indicator_name='RSI',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная медвежья дивергенция: цена формирует более высокие максимумы, RSI формирует более низкие максимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Поиск скрытых бычьих дивергенций
            # Цена: более высокие минимумы, RSI: более низкие минимумы
            for i in range(len(price_troughs) - 1):
                for j in range(len(rsi_troughs) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_troughs[i] - rsi_troughs[j]) <= self.window and abs(price_troughs[i+1] - rsi_troughs[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_troughs[i+1]] > price.iloc[price_troughs[i]] and rsi.iloc[rsi_troughs[j+1]] < rsi.iloc[rsi_troughs[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_troughs[i+1]] - price.iloc[price_troughs[i]]) / price.iloc[price_troughs[i]]
                            rsi_change = (rsi.iloc[rsi_troughs[j+1]] - rsi.iloc[rsi_troughs[j]]) / rsi.iloc[rsi_troughs[j]]
                            
                            if abs(price_change) > self.min_threshold and abs(rsi_change) > self.min_threshold:
                                # Определяем силу дивергенции
                                strength = min(0.5 + (abs(price_change) + abs(rsi_change)) / 2, 1.0)
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.HIDDEN_BULLISH,
                                    price_point1=price_troughs[i],
                                    price_point2=price_troughs[i+1],
                                    indicator_point1=rsi_troughs[j],
                                    indicator_point2=rsi_troughs[j+1],
                                    indicator_name='RSI',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Скрытая бычья дивергенция: цена формирует более высокие минимумы, RSI формирует более низкие минимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Поиск скрытых медвежьих дивергенций
            # Цена: более низкие максимумы, RSI: более высокие максимумы
            for i in range(len(price_peaks) - 1):
                for j in range(len(rsi_peaks) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_peaks[i] - rsi_peaks[j]) <= self.window and abs(price_peaks[i+1] - rsi_peaks[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_peaks[i+1]] < price.iloc[price_peaks[i]] and rsi.iloc[rsi_peaks[j+1]] > rsi.iloc[rsi_peaks[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_peaks[i+1]] - price.iloc[price_peaks[i]]) / price.iloc[price_peaks[i]]
                            rsi_change = (rsi.iloc[rsi_peaks[j+1]] - rsi.iloc[rsi_peaks[j]]) / rsi.iloc[rsi_peaks[j]]
                            
                            if abs(price_change) > self.min_threshold and abs(rsi_change) > self.min_threshold:
                                # Определяем силу дивергенции
                                strength = min(0.5 + (abs(price_change) + abs(rsi_change)) / 2, 1.0)
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.HIDDEN_BEARISH,
                                    price_point1=price_peaks[i],
                                    price_point2=price_peaks[i+1],
                                    indicator_point1=rsi_peaks[j],
                                    indicator_point2=rsi_peaks[j+1],
                                    indicator_name='RSI',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Скрытая медвежья дивергенция: цена формирует более низкие максимумы, RSI формирует более высокие максимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Сохраняем дивергенции
            self.divergences = divergences
            
            return divergences
        except Exception as e:
            logger.error(f"Ошибка при обнаружении дивергенций RSI: {e}")
            return []

class MACDDivergenceDetector(BaseDivergenceDetector):
    """
    Детектор дивергенций MACD
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора дивергенций MACD
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.name = 'MACDDivergence'
        
        # Настройка параметров
        self.window = self.params.get('window', 5)  # Окно для поиска экстремумов
        self.min_divergence_points = self.params.get('min_divergence_points', 2)  # Минимальное количество точек для дивергенции
        self.min_threshold = self.params.get('min_threshold', 0.03)  # Минимальный порог для дивергенции (3%)
    
    @property
    def required_indicators(self) -> List[str]:
        """
        Список индикаторов, необходимых для обнаружения дивергенций
        
        Returns:
            List[str]: Список индикаторов
        """
        return ['MACD']
    
    def detect(self, df: pd.DataFrame, indicator_data: Dict[str, pd.Series]) -> List[Divergence]:
        """
        Обнаружение дивергенций между ценой и MACD
        
        Args:
            df: DataFrame с данными
            indicator_data: Словарь с данными индикаторов
            
        Returns:
            List[Divergence]: Список обнаруженных дивергенций
        """
        if not self.validate_data(df, indicator_data):
            return []
        
        divergences = []
        
        try:
            # Получаем цену и индикатор
            price = df['close']
            
            # Получаем MACD (может быть вложен в индикаторе)
            macd_line = None
            if 'MACD' in indicator_data:
                if isinstance(indicator_data['MACD'], dict) and 'macd_line' in indicator_data['MACD']:
                    macd_line = indicator_data['MACD']['macd_line']
                elif isinstance(indicator_data['MACD'], dict) and 'MACD_line' in indicator_data['MACD']:
                    macd_line = indicator_data['MACD']['MACD_line']
                else:
                    macd_line = indicator_data['MACD']
            
            if macd_line is None or len(macd_line) != len(price):
                logger.warning(f"{self.name}: MACD не найден или имеет неверную длину")
                return []
            
            # Поиск локальных экстремумов для цены
            price_peaks = []
            price_troughs = []
            
            for i in range(self.window, len(price) - self.window):
                # Локальный максимум
                if all(price.iloc[i] > price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] > price.iloc[i+j] for j in range(1, self.window+1)):
                    price_peaks.append(i)
                
                # Локальный минимум
                if all(price.iloc[i] < price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] < price.iloc[i+j] for j in range(1, self.window+1)):
                    price_troughs.append(i)
            
            # Поиск локальных экстремумов для MACD
            macd_peaks = []
            macd_troughs = []
            
            for i in range(self.window, len(macd_line) - self.window):
                # Локальный максимум
                if all(macd_line.iloc[i] > macd_line.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(macd_line.iloc[i] > macd_line.iloc[i+j] for j in range(1, self.window+1)):
                    macd_peaks.append(i)
                
                # Локальный минимум
                if all(macd_line.iloc[i] < macd_line.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(macd_line.iloc[i] < macd_line.iloc[i+j] for j in range(1, self.window+1)):
                    macd_troughs.append(i)
            
            # Поиск регулярных бычьих дивергенций
            # Цена: более низкие минимумы, MACD: более высокие минимумы
            for i in range(len(price_troughs) - 1):
                for j in range(len(macd_troughs) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_troughs[i] - macd_troughs[j]) <= self.window and abs(price_troughs[i+1] - macd_troughs[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_troughs[i+1]] < price.iloc[price_troughs[i]] and macd_line.iloc[macd_troughs[j+1]] > macd_line.iloc[macd_troughs[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_troughs[i+1]] - price.iloc[price_troughs[i]]) / price.iloc[price_troughs[i]]
                            
                            # Для MACD используем относительное изменение
                            macd_change = macd_line.iloc[macd_troughs[j+1]] - macd_line.iloc[macd_troughs[j]]
                            
                            if abs(price_change) > self.min_threshold and abs(macd_change) > 0.001:
                                # Определяем силу дивергенции на основе разницы в цене
                                strength = min(0.5 + abs(price_change) / 0.1, 1.0)  # Максимальная сила при 10% изменении цены
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BULLISH,
                                    price_point1=price_troughs[i],
                                    price_point2=price_troughs[i+1],
                                    indicator_point1=macd_troughs[j],
                                    indicator_point2=macd_troughs[j+1],
                                    indicator_name='MACD',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная бычья дивергенция: цена формирует более низкие минимумы, MACD формирует более высокие минимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Поиск регулярных медвежьих дивергенций
            # Цена: более высокие максимумы, MACD: более низкие максимумы
            for i in range(len(price_peaks) - 1):
                for j in range(len(macd_peaks) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_peaks[i] - macd_peaks[j]) <= self.window and abs(price_peaks[i+1] - macd_peaks[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_peaks[i+1]] > price.iloc[price_peaks[i]] and macd_line.iloc[macd_peaks[j+1]] < macd_line.iloc[macd_peaks[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_peaks[i+1]] - price.iloc[price_peaks[i]]) / price.iloc[price_peaks[i]]
                            
                            # Для MACD используем относительное изменение
                            macd_change = macd_line.iloc[macd_peaks[j+1]] - macd_line.iloc[macd_peaks[j]]
                            
                            if abs(price_change) > self.min_threshold and abs(macd_change) > 0.001:
                                # Определяем силу дивергенции на основе разницы в цене
                                strength = min(0.5 + abs(price_change) / 0.1, 1.0)  # Максимальная сила при 10% изменении цены
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BEARISH,
                                    price_point1=price_peaks[i],
                                    price_point2=price_peaks[i+1],
                                    indicator_point1=macd_peaks[j],
                                    indicator_point2=macd_peaks[j+1],
                                    indicator_name='MACD',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная медвежья дивергенция: цена формирует более высокие максимумы, MACD формирует более низкие максимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Сохраняем дивергенции
            self.divergences = divergences
            
            return divergences
        except Exception as e:
            logger.error(f"Ошибка при обнаружении дивергенций MACD: {e}")
            return []

class StochasticDivergenceDetector(BaseDivergenceDetector):
    """
    Детектор дивергенций Stochastic
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора дивергенций Stochastic
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.name = 'StochasticDivergence'
        
        # Настройка параметров
        self.window = self.params.get('window', 5)  # Окно для поиска экстремумов
        self.min_divergence_points = self.params.get('min_divergence_points', 2)  # Минимальное количество точек для дивергенции
        self.min_threshold = self.params.get('min_threshold', 0.03)  # Минимальный порог для дивергенции (3%)
    
    @property
    def required_indicators(self) -> List[str]:
        """
        Список индикаторов, необходимых для обнаружения дивергенций
        
        Returns:
            List[str]: Список индикаторов
        """
        return ['Stochastic']
    
    def detect(self, df: pd.DataFrame, indicator_data: Dict[str, pd.Series]) -> List[Divergence]:
        """
        Обнаружение дивергенций между ценой и Stochastic
        
        Args:
            df: DataFrame с данными
            indicator_data: Словарь с данными индикаторов
            
        Returns:
            List[Divergence]: Список обнаруженных дивергенций
        """
        if not self.validate_data(df, indicator_data):
            return []
        
        divergences = []
        
        try:
            # Получаем цену и индикатор
            price = df['close']
            
            # Получаем Stochastic (может быть вложен в индикаторе)
            stoch_k = None
            if 'Stochastic' in indicator_data:
                if isinstance(indicator_data['Stochastic'], dict) and 'k' in indicator_data['Stochastic']:
                    stoch_k = indicator_data['Stochastic']['k']
                else:
                    stoch_k = indicator_data['Stochastic']
            
            if stoch_k is None or len(stoch_k) != len(price):
                logger.warning(f"{self.name}: Stochastic не найден или имеет неверную длину")
                return []
            
            # Поиск локальных экстремумов для цены
            price_peaks = []
            price_troughs = []
            
            for i in range(self.window, len(price) - self.window):
                # Локальный максимум
                if all(price.iloc[i] > price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] > price.iloc[i+j] for j in range(1, self.window+1)):
                    price_peaks.append(i)
                
                # Локальный минимум
                if all(price.iloc[i] < price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] < price.iloc[i+j] for j in range(1, self.window+1)):
                    price_troughs.append(i)
            
            # Поиск локальных экстремумов для Stochastic
            stoch_peaks = []
            stoch_troughs = []
            
            for i in range(self.window, len(stoch_k) - self.window):
                # Локальный максимум
                if all(stoch_k.iloc[i] > stoch_k.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(stoch_k.iloc[i] > stoch_k.iloc[i+j] for j in range(1, self.window+1)):
                    stoch_peaks.append(i)
                
                # Локальный минимум
                if all(stoch_k.iloc[i] < stoch_k.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(stoch_k.iloc[i] < stoch_k.iloc[i+j] for j in range(1, self.window+1)):
                    stoch_troughs.append(i)
            
            # Поиск регулярных бычьих дивергенций
            # Цена: более низкие минимумы, Stochastic: более высокие минимумы
            for i in range(len(price_troughs) - 1):
                for j in range(len(stoch_troughs) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_troughs[i] - stoch_troughs[j]) <= self.window and abs(price_troughs[i+1] - stoch_troughs[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_troughs[i+1]] < price.iloc[price_troughs[i]] and stoch_k.iloc[stoch_troughs[j+1]] > stoch_k.iloc[stoch_troughs[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_troughs[i+1]] - price.iloc[price_troughs[i]]) / price.iloc[price_troughs[i]]
                            stoch_change = (stoch_k.iloc[stoch_troughs[j+1]] - stoch_k.iloc[stoch_troughs[j]])
                            
                            if abs(price_change) > self.min_threshold and abs(stoch_change) > 5:  # 5 пунктов стохастика
                                # Определяем силу дивергенции
                                strength = min(0.5 + abs(price_change) / 0.1, 1.0)  # Максимальная сила при 10% изменении цены
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BULLISH,
                                    price_point1=price_troughs[i],
                                    price_point2=price_troughs[i+1],
                                    indicator_point1=stoch_troughs[j],
                                    indicator_point2=stoch_troughs[j+1],
                                    indicator_name='Stochastic',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная бычья дивергенция: цена формирует более низкие минимумы, Stochastic формирует более высокие минимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Поиск регулярных медвежьих дивергенций
            # Цена: более высокие максимумы, Stochastic: более низкие максимумы
            for i in range(len(price_peaks) - 1):
                for j in range(len(stoch_peaks) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_peaks[i] - stoch_peaks[j]) <= self.window and abs(price_peaks[i+1] - stoch_peaks[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_peaks[i+1]] > price.iloc[price_peaks[i]] and stoch_k.iloc[stoch_peaks[j+1]] < stoch_k.iloc[stoch_peaks[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_peaks[i+1]] - price.iloc[price_peaks[i]]) / price.iloc[price_peaks[i]]
                            stoch_change = (stoch_k.iloc[stoch_peaks[j+1]] - stoch_k.iloc[stoch_peaks[j]])
                            
                            if abs(price_change) > self.min_threshold and abs(stoch_change) > 5:  # 5 пунктов стохастика
                                # Определяем силу дивергенции
                                strength = min(0.5 + abs(price_change) / 0.1, 1.0)  # Максимальная сила при 10% изменении цены
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BEARISH,
                                    price_point1=price_peaks[i],
                                    price_point2=price_peaks[i+1],
                                    indicator_point1=stoch_peaks[j],
                                    indicator_point2=stoch_peaks[j+1],
                                    indicator_name='Stochastic',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная медвежья дивергенция: цена формирует более высокие максимумы, Stochastic формирует более низкие максимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Сохраняем дивергенции
            self.divergences = divergences
            
            return divergences
        except Exception as e:
            logger.error(f"Ошибка при обнаружении дивергенций Stochastic: {e}")
            return []

class OBVDivergenceDetector(BaseDivergenceDetector):
    """
    Детектор дивергенций OBV
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация детектора дивергенций OBV
        
        Args:
            params: Параметры детектора
        """
        super().__init__(params)
        self.name = 'OBVDivergence'
        
        # Настройка параметров
        self.window = self.params.get('window', 5)  # Окно для поиска экстремумов
        self.min_divergence_points = self.params.get('min_divergence_points', 2)  # Минимальное количество точек для дивергенции
        self.min_threshold = self.params.get('min_threshold', 0.03)  # Минимальный порог для дивергенции (3%)
    
    @property
    def required_indicators(self) -> List[str]:
        """
        Список индикаторов, необходимых для обнаружения дивергенций
        
        Returns:
            List[str]: Список индикаторов
        """
        return ['OBV']
    
    def detect(self, df: pd.DataFrame, indicator_data: Dict[str, pd.Series]) -> List[Divergence]:
        """
        Обнаружение дивергенций между ценой и OBV
        
        Args:
            df: DataFrame с данными
            indicator_data: Словарь с данными индикаторов
            
        Returns:
            List[Divergence]: Список обнаруженных дивергенций
        """
        if not self.validate_data(df, indicator_data):
            return []
        
        divergences = []
        
        try:
            # Получаем цену и индикатор
            price = df['close']
            
            # Получаем OBV (может быть вложен в индикаторе)
            obv = None
            if 'OBV' in indicator_data:
                if isinstance(indicator_data['OBV'], dict) and 'obv' in indicator_data['OBV']:
                    obv = indicator_data['OBV']['obv']
                else:
                    obv = indicator_data['OBV']
            
            if obv is None or len(obv) != len(price):
                logger.warning(f"{self.name}: OBV не найден или имеет неверную длину")
                return []
            
            # Поиск локальных экстремумов для цены
            price_peaks = []
            price_troughs = []
            
            for i in range(self.window, len(price) - self.window):
                # Локальный максимум
                if all(price.iloc[i] > price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] > price.iloc[i+j] for j in range(1, self.window+1)):
                    price_peaks.append(i)
                
                # Локальный минимум
                if all(price.iloc[i] < price.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(price.iloc[i] < price.iloc[i+j] for j in range(1, self.window+1)):
                    price_troughs.append(i)
            
            # Поиск локальных экстремумов для OBV
            obv_peaks = []
            obv_troughs = []
            
            for i in range(self.window, len(obv) - self.window):
                # Локальный максимум
                if all(obv.iloc[i] > obv.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(obv.iloc[i] > obv.iloc[i+j] for j in range(1, self.window+1)):
                    obv_peaks.append(i)
                
                # Локальный минимум
                if all(obv.iloc[i] < obv.iloc[i-j] for j in range(1, self.window+1)) and \
                   all(obv.iloc[i] < obv.iloc[i+j] for j in range(1, self.window+1)):
                    obv_troughs.append(i)
            
            # Поиск регулярных бычьих дивергенций
            # Цена: более низкие минимумы, OBV: более высокие минимумы
            for i in range(len(price_troughs) - 1):
                for j in range(len(obv_troughs) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_troughs[i] - obv_troughs[j]) <= self.window and abs(price_troughs[i+1] - obv_troughs[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_troughs[i+1]] < price.iloc[price_troughs[i]] and obv.iloc[obv_troughs[j+1]] > obv.iloc[obv_troughs[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_troughs[i+1]] - price.iloc[price_troughs[i]]) / price.iloc[price_troughs[i]]
                            obv_change = (obv.iloc[obv_troughs[j+1]] - obv.iloc[obv_troughs[j]]) / abs(obv.iloc[obv_troughs[j]]) if obv.iloc[obv_troughs[j]] != 0 else 1.0
                            
                            if abs(price_change) > self.min_threshold and abs(obv_change) > 0.01:  # 1% изменение OBV
                                # Определяем силу дивергенции
                                strength = min(0.5 + abs(price_change) / 0.1, 1.0)  # Максимальная сила при 10% изменении цены
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BULLISH,
                                    price_point1=price_troughs[i],
                                    price_point2=price_troughs[i+1],
                                    indicator_point1=obv_troughs[j],
                                    indicator_point2=obv_troughs[j+1],
                                    indicator_name='OBV',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная бычья дивергенция: цена формирует более низкие минимумы, OBV формирует более высокие минимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Поиск регулярных медвежьих дивергенций
            # Цена: более высокие максимумы, OBV: более низкие максимумы
            for i in range(len(price_peaks) - 1):
                for j in range(len(obv_peaks) - 1):
                    # Проверяем, что экстремумы рядом
                    if abs(price_peaks[i] - obv_peaks[j]) <= self.window and abs(price_peaks[i+1] - obv_peaks[j+1]) <= self.window:
                        # Проверяем дивергенцию
                        if price.iloc[price_peaks[i+1]] > price.iloc[price_peaks[i]] and obv.iloc[obv_peaks[j+1]] < obv.iloc[obv_peaks[j]]:
                            # Проверяем, что дивергенция значительна
                            price_change = (price.iloc[price_peaks[i+1]] - price.iloc[price_peaks[i]]) / price.iloc[price_peaks[i]]
                            obv_change = (obv.iloc[obv_peaks[j+1]] - obv.iloc[obv_peaks[j]]) / abs(obv.iloc[obv_peaks[j]]) if obv.iloc[obv_peaks[j]] != 0 else 1.0
                            
                            if abs(price_change) > self.min_threshold and abs(obv_change) > 0.01:  # 1% изменение OBV
                                # Определяем силу дивергенции
                                strength = min(0.5 + abs(price_change) / 0.1, 1.0)  # Максимальная сила при 10% изменении цены
                                
                                # Создаем дивергенцию
                                divergence = Divergence(
                                    type=DivergenceType.REGULAR_BEARISH,
                                    price_point1=price_peaks[i],
                                    price_point2=price_peaks[i+1],
                                    indicator_point1=obv_peaks[j],
                                    indicator_point2=obv_peaks[j+1],
                                    indicator_name='OBV',
                                    strength=strength,
                                    confirmed=True,
                                    description=f"Регулярная медвежья дивергенция: цена формирует более высокие максимумы, OBV формирует более низкие максимумы"
                                )
                                
                                divergences.append(divergence)
            
            # Сохраняем дивергенции
            self.divergences = divergences
            
            return divergences
        except Exception as e:
            logger.error(f"Ошибка при обнаружении дивергенций OBV: {e}")
            return []

# ===========================================================================
# БЛОК 10: РЕАЛИЗАЦИЯ АНАЛИЗАТОРОВ ОРДЕРБУКОВ
# ===========================================================================

class LiquidityAnalyzer(OrderbookAnalyzer):
    """
    Анализатор ликвидности ордербука
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация анализатора ликвидности
        
        Args:
            params: Параметры анализатора
        """
        super().__init__(params)
        
        # Настройка параметров
        self.volume_threshold = self.params.get('volume_threshold', 2.0)  # Порог для кластеров ликвидности
        self.levels_to_detect = self.params.get('levels_to_detect', 5)  # Количество уровней ликвидности для обнаружения
    
    def analyze(self, orderbook_data: Dict) -> Dict:
        """
        Анализ ликвидности в ордербуке
        
        Args:
            orderbook_data: Данные ордербука
            
        Returns:
            Dict: Результаты анализа
        """
        if not self.validate_data(orderbook_data):
            return {}
        
        try:
            # Извлекаем бид и аск из ордербука
            bids = orderbook_data.get('bids', [])
            asks = orderbook_data.get('asks', [])
            
            if not bids or not asks:
                return {}
            
            # Преобразуем в массивы numpy для быстрой обработки
            bid_prices = np.array([float(bid[0]) for bid in bids])
            bid_volumes = np.array([float(bid[1]) for bid in bids])
            ask_prices = np.array([float(ask[0]) for ask in asks])
            ask_volumes = np.array([float(ask[1]) for ask in asks])
            
            # Вычисляем общий и средний объем
            total_bid_volume = np.sum(bid_volumes)
            total_ask_volume = np.sum(ask_volumes)
            avg_bid_volume = np.mean(bid_volumes)
            avg_ask_volume = np.mean(ask_volumes)
            
            # Идентификация кластеров ликвидности
            # Ищем уровни с объемом выше среднего
            bid_liquidity_clusters = []
            ask_liquidity_clusters = []
            
            # Кластеры ликвидности в бидах
            for i in range(len(bid_prices)):
                if bid_volumes[i] > avg_bid_volume * self.volume_threshold:
                    bid_liquidity_clusters.append({
                        'price': bid_prices[i],
                        'volume': bid_volumes[i],
                        'relative_volume': bid_volumes[i] / avg_bid_volume
                    })
            
            # Кластеры ликвидности в асках
            for i in range(len(ask_prices)):
                if ask_volumes[i] > avg_ask_volume * self.volume_threshold:
                    ask_liquidity_clusters.append({
                        'price': ask_prices[i],
                        'volume': ask_volumes[i],
                        'relative_volume': ask_volumes[i] / avg_ask_volume
                    })
            
            # Сортируем по объему и выбираем топ N уровней
            bid_liquidity_clusters.sort(key=lambda x: x['volume'], reverse=True)
            ask_liquidity_clusters.sort(key=lambda x: x['volume'], reverse=True)
            
            bid_liquidity_levels = bid_liquidity_clusters[:self.levels_to_detect]
            ask_liquidity_levels = ask_liquidity_clusters[:self.levels_to_detect]
            
            # Вычисляем глубину рынка (объем в диапазоне X% от текущей цены)
            spread = ask_prices[0] - bid_prices[0]
            spread_percent = spread / bid_prices[0] * 100
            
            # Рассчитываем объем в пределах 1% от текущей цены
            price_range = 0.01  # 1%
            
            # Объем в пределах 1% для бидов
            bid_depth_volume = sum(volume for price, volume in zip(bid_prices, bid_volumes) if bid_prices[0] - price * price_range <= price <= bid_prices[0])
            
            # Объем в пределах 1% для асков
            ask_depth_volume = sum(volume for price, volume in zip(ask_prices, ask_volumes) if ask_prices[0] <= price <= ask_prices[0] + price * price_range)
            
            # Сохраняем результаты
            self.results = {
                'bid_liquidity_levels': bid_liquidity_levels,
                'ask_liquidity_levels': ask_liquidity_levels,
                'spread': spread,
                'spread_percent': spread_percent,
                'bid_depth_volume': bid_depth_volume,
                'ask_depth_volume': ask_depth_volume,
                'total_bid_volume': total_bid_volume,
                'total_ask_volume': total_ask_volume,
                'bid_ask_ratio': total_bid_volume / total_ask_volume if total_ask_volume > 0 else float('inf')
            }
            
            return self.results
        except Exception as e:
            logger.error(f"Ошибка при анализе ликвидности: {e}")
            return {}
    
    def generate_signals(self, orderbook_data: Dict, price_data: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе анализа ликвидности ордербука
        
        Args:
            orderbook_data: Данные ордербука
            price_data: Данные о ценах
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if not self.validate_data(orderbook_data):
            return signals
        
        try:
            # Анализируем ордербук, если еще не сделано
            if not self.results:
                self.analyze(orderbook_data)
            
            # Если анализ не удался, выходим
            if not self.results:
                return signals
            
            # Получаем текущую цену закрытия
            if len(price_data) > 0:
                current_price = price_data['close'].iloc[-1]
            else:
                # Используем среднюю цену между лучшими бидом и аском
                best_bid = orderbook_data['bids'][0][0] if orderbook_data.get('bids') else 0
                best_ask = orderbook_data['asks'][0][0] if orderbook_data.get('asks') else 0
                current_price = (float(best_bid) + float(best_ask)) / 2
            
            # Генерация сигналов на основе кластеров ликвидности
            
            # Сигнал на основе сильной поддержки (кластеры ликвидности в бидах)
            if self.results.get('bid_liquidity_levels'):
                # Проверяем, есть ли кластер близко к текущей цене
                for level in self.results['bid_liquidity_levels']:
                    price_distance = (current_price - level['price']) / current_price * 100  # В процентах
                    
                    # Если цена близко к кластеру поддержки (в пределах 1%)
                    if 0 < price_distance <= 1.0:
                        # Сила сигнала зависит от объема кластера
                        strength = min(0.5 + level['relative_volume'] / 10, 0.9)
                        
                        signal = Signal(
                            type=SignalType.BUY,
                            strength=strength,
                            indicator="LiquidityCluster_Support",
                            timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                            timeframe="",
                            price=current_price,
                            description=f"Сильный кластер ликвидности (поддержка) на уровне {level['price']:.8f} ({level['relative_volume']:.2f}x от среднего объема)",
                            stop_loss=level['price'] * 0.99,  # Стоп-лосс немного ниже уровня поддержки
                            take_profit=current_price * 1.03  # Тейк-профит на 3% выше текущей цены
                        )
                        
                        signals.append(signal)
            
            # Сигнал на основе сильного сопротивления (кластеры ликвидности в асках)
            if self.results.get('ask_liquidity_levels'):
                # Проверяем, есть ли кластер близко к текущей цене
                for level in self.results['ask_liquidity_levels']:
                    price_distance = (level['price'] - current_price) / current_price * 100  # В процентах
                    
                    # Если цена близко к кластеру сопротивления (в пределах 1%)
                    if 0 < price_distance <= 1.0:
                        # Сила сигнала зависит от объема кластера
                        strength = min(0.5 + level['relative_volume'] / 10, 0.9)
                        
                        signal = Signal(
                            type=SignalType.SELL,
                            strength=strength,
                            indicator="LiquidityCluster_Resistance",
                            timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                            timeframe="",
                            price=current_price,
                            description=f"Сильный кластер ликвидности (сопротивление) на уровне {level['price']:.8f} ({level['relative_volume']:.2f}x от среднего объема)",
                            stop_loss=level['price'] * 1.01,  # Стоп-лосс немного выше уровня сопротивления
                            take_profit=current_price * 0.97  # Тейк-профит на 3% ниже текущей цены
                        )
                        
                        signals.append(signal)
            
            # Сигнал на основе дисбаланса бид/аск
            bid_ask_ratio = self.results.get('bid_ask_ratio', 1.0)
            
            if bid_ask_ratio > 2.0:  # Значительный перевес покупателей
                signal = Signal(
                    type=SignalType.BUY,
                    strength=min(0.5 + bid_ask_ratio / 10, 0.9),
                    indicator="Liquidity_Imbalance",
                    timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                    timeframe="",
                    price=current_price,
                    description=f"Дисбаланс ликвидности: объем бидов превышает объем асков в {bid_ask_ratio:.2f} раз",
                    stop_loss=current_price * 0.98,  # Стоп-лосс на 2% ниже текущей цены
                    take_profit=current_price * 1.04  # Тейк-профит на 4% выше текущей цены
                )
                
                signals.append(signal)
            elif bid_ask_ratio < 0.5:  # Значительный перевес продавцов
                signal = Signal(
                    type=SignalType.SELL,
                    strength=min(0.5 + (1 / bid_ask_ratio) / 10, 0.9),
                    indicator="Liquidity_Imbalance",
                    timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                    timeframe="",
                    price=current_price,
                    description=f"Дисбаланс ликвидности: объем асков превышает объем бидов в {1 / bid_ask_ratio:.2f} раз",
                    stop_loss=current_price * 1.02,  # Стоп-лосс на 2% выше текущей цены
                    take_profit=current_price * 0.96  # Тейк-профит на 4% ниже текущей цены
                )
                
                signals.append(signal)
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов на основе анализа ликвидности: {e}")
            return []

class ImbalanceAnalyzer(OrderbookAnalyzer):
    """
    Анализатор имбаланса ордербука
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация анализатора имбаланса
        
        Args:
            params: Параметры анализатора
        """
        super().__init__(params)
        
        # Настройка параметров
        self.imbalance_threshold = self.params.get('imbalance_threshold', 2.0)  # Порог для имбаланса
        self.price_level_groups = self.params.get('price_level_groups', 5)  # Количество групп цен для анализа
    
    def analyze(self, orderbook_data: Dict) -> Dict:
        """
        Анализ имбаланса в ордербуке
        
        Args:
            orderbook_data: Данные ордербука
            
        Returns:
            Dict: Результаты анализа
        """
        if not self.validate_data(orderbook_data):
            return {}
        
        try:
            # Извлекаем бид и аск из ордербука
            bids = orderbook_data.get('bids', [])
            asks = orderbook_data.get('asks', [])
            
            if not bids or not asks:
                return {}
            
            # Преобразуем в массивы numpy для быстрой обработки
            bid_prices = np.array([float(bid[0]) for bid in bids])
            bid_volumes = np.array([float(bid[1]) for bid in bids])
            ask_prices = np.array([float(ask[0]) for ask in asks])
            ask_volumes = np.array([float(ask[1]) for ask in asks])
            
            # Расчет спреда
            best_bid = bid_prices[0]
            best_ask = ask_prices[0]
            spread = best_ask - best_bid
            spread_percent = spread / best_bid * 100
            
            # Вычисляем общий объем
            total_bid_volume = np.sum(bid_volumes)
            total_ask_volume = np.sum(ask_volumes)
            
            # Расчет соотношения бид/аск
            bid_ask_ratio = total_bid_volume / total_ask_volume if total_ask_volume > 0 else float('inf')
            
            # Разделение ордербука на группы уровней цен
            # Например, для 5 групп: 0-1%, 1-2%, 2-3%, 3-4%, 4-5%
            
            # Для бидов (цены ниже best_bid)
            bid_price_range = best_bid * 0.05  # 5% от текущей цены
            bid_group_size = bid_price_range / self.price_level_groups
            
            bid_groups = []
            for i in range(self.price_level_groups):
                # Верхняя и нижняя граница для группы
                upper_bound = best_bid - i * bid_group_size
                lower_bound = best_bid - (i + 1) * bid_group_size
                
                # Находим все заявки в этом ценовом диапазоне
                group_mask = (bid_prices <= upper_bound) & (bid_prices > lower_bound)
                group_volume = np.sum(bid_volumes[group_mask])
                
                bid_groups.append({
                    'upper_bound': upper_bound,
                    'lower_bound': lower_bound,
                    'volume': group_volume
                })
            
            # Для асков (цены выше best_ask)
            ask_price_range = best_ask * 0.05  # 5% от текущей цены
            ask_group_size = ask_price_range / self.price_level_groups
            
            ask_groups = []
            for i in range(self.price_level_groups):
                # Нижняя и верхняя граница для группы
                lower_bound = best_ask + i * ask_group_size
                upper_bound = best_ask + (i + 1) * ask_group_size
                
                # Находим все заявки в этом ценовом диапазоне
                group_mask = (ask_prices >= lower_bound) & (ask_prices < upper_bound)
                group_volume = np.sum(ask_volumes[group_mask])
                
                ask_groups.append({
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound,
                    'volume': group_volume
                })
            
            # Поиск имбалансов между группами
            imbalances = []
            
            # Сравниваем объемы в соответствующих группах бидов и асков
            for i in range(min(len(bid_groups), len(ask_groups))):
                bid_volume = bid_groups[i]['volume']
                ask_volume = ask_groups[i]['volume']
                
                if bid_volume > 0 and ask_volume > 0:
                    ratio = bid_volume / ask_volume
                    
                    # Если есть значительный имбаланс
                    if ratio > self.imbalance_threshold or ratio < (1 / self.imbalance_threshold):
                        imbalances.append({
                            'group_index': i,
                            'bid_volume': bid_volume,
                            'ask_volume': ask_volume,
                            'ratio': ratio,
                            'is_bid_dominant': ratio > 1,
                            'level': (bid_groups[i]['upper_bound'] + ask_groups[i]['lower_bound']) / 2
                        })
            
            # Сохраняем результаты
            self.results = {
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'spread_percent': spread_percent,
                'bid_ask_ratio': bid_ask_ratio,
                'bid_groups': bid_groups,
                'ask_groups': ask_groups,
                'imbalances': imbalances
            }
            
            return self.results
        except Exception as e:
            logger.error(f"Ошибка при анализе имбаланса: {e}")
            return {}
    
    def generate_signals(self, orderbook_data: Dict, price_data: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе анализа имбаланса ордербука
        
        Args:
            orderbook_data: Данные ордербука
            price_data: Данные о ценах
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if not self.validate_data(orderbook_data):
            return signals
        
        try:
            # Анализируем ордербук, если еще не сделано
            if not self.results:
                self.analyze(orderbook_data)
            
            # Если анализ не удался, выходим
            if not self.results:
                return signals
            
            # Получаем текущую цену закрытия
            if len(price_data) > 0:
                current_price = price_data['close'].iloc[-1]
            else:
                # Используем среднюю цену между лучшими бидом и аском
                best_bid = self.results.get('best_bid', 0)
                best_ask = self.results.get('best_ask', 0)
                current_price = (best_bid + best_ask) / 2
            
            # Генерация сигналов на основе имбалансов
            
            # Общий дисбаланс бид/аск
            bid_ask_ratio = self.results.get('bid_ask_ratio', 1.0)
            
            if bid_ask_ratio > self.imbalance_threshold:
                signal = Signal(
                    type=SignalType.BUY,
                    strength=min(0.5 + bid_ask_ratio / 10, 0.9),
                    indicator="OrderbookImbalance",
                    timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                    timeframe="",
                    price=current_price,
                    description=f"Общий имбаланс в ордербуке: покупатели доминируют ({bid_ask_ratio:.2f}x)",
                    stop_loss=current_price * 0.98,  # Стоп-лосс на 2% ниже текущей цены
                    take_profit=current_price * 1.04  # Тейк-профит на 4% выше текущей цены
                )
                
                signals.append(signal)
            elif bid_ask_ratio < (1 / self.imbalance_threshold):
                signal = Signal(
                    type=SignalType.SELL,
                    strength=min(0.5 + (1 / bid_ask_ratio) / 10, 0.9),
                    indicator="OrderbookImbalance",
                    timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                    timeframe="",
                    price=current_price,
                    description=f"Общий имбаланс в ордербуке: продавцы доминируют ({1 / bid_ask_ratio:.2f}x)",
                    stop_loss=current_price * 1.02,  # Стоп-лосс на 2% выше текущей цены
                    take_profit=current_price * 0.96  # Тейк-профит на 4% ниже текущей цены
                )
                
                signals.append(signal)
            
            # Имбалансы по группам
            if self.results.get('imbalances'):
                for imbalance in self.results['imbalances']:
                    # Проверяем, насколько близко имбаланс к текущей цене
                    imbalance_level = imbalance['level']
                    distance_percent = abs(imbalance_level - current_price) / current_price * 100
                    
                    # Имбаланс значим, если он рядом с текущей ценой
                    if distance_percent <= 3.0:  # В пределах 3%
                        # Сила сигнала зависит от расстояния и соотношения
                        ratio = imbalance['ratio'] if imbalance['is_bid_dominant'] else 1 / imbalance['ratio']
                        strength = min(0.5 + (ratio / 5) * (1 - distance_percent / 5), 0.9)
                        
                        if imbalance['is_bid_dominant']:
                            signal = Signal(
                                type=SignalType.BUY,
                                strength=strength,
                                indicator=f"ImbalanceGroup_{imbalance['group_index']}",
                                timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                                timeframe="",
                                price=current_price,
                                description=f"Имбаланс в группе {imbalance['group_index']}: покупатели доминируют ({imbalance['ratio']:.2f}x) на уровне {imbalance_level:.8f}",
                                stop_loss=current_price * 0.98,  # Стоп-лосс на 2% ниже текущей цены
                                take_profit=current_price * 1.03  # Тейк-профит на 3% выше текущей цены
                            )
                            
                            signals.append(signal)
                        else:
                            signal = Signal(
                                type=SignalType.SELL,
                                strength=strength,
                                indicator=f"ImbalanceGroup_{imbalance['group_index']}",
                                timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                                timeframe="",
                                price=current_price,
                                description=f"Имбаланс в группе {imbalance['group_index']}: продавцы доминируют ({1 / imbalance['ratio']:.2f}x) на уровне {imbalance_level:.8f}",
                                stop_loss=current_price * 1.02,  # Стоп-лосс на 2% выше текущей цены
                                take_profit=current_price * 0.97  # Тейк-профит на 3% ниже текущей цены
                            )
                            
                            signals.append(signal)
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов на основе анализа имбаланса: {e}")
            return []

class SupportResistanceAnalyzer(OrderbookAnalyzer):
    """
    Анализатор уровней поддержки и сопротивления на основе ордербука
    """
    
    def __init__(self, params: Dict = None):
        """
        Инициализация анализатора уровней поддержки и сопротивления
        
        Args:
            params: Параметры анализатора
        """
        super().__init__(params)
        
        # Настройка параметров
        self.volume_threshold = self.params.get('volume_threshold', 2.0)  # Порог для уровней
        self.max_levels = self.params.get('max_levels', 5)  # Максимальное количество уровней для определения
        self.price_grouping = self.params.get('price_grouping', 0.005)  # Группировка цен (0.5%)
    
    def analyze(self, orderbook_data: Dict) -> Dict:
        """
        Анализ уровней поддержки и сопротивления в ордербуке
        
        Args:
            orderbook_data: Данные ордербука
            
        Returns:
            Dict: Результаты анализа
        """
        if not self.validate_data(orderbook_data):
            return {}
        
        try:
            # Извлекаем бид и аск из ордербука
            bids = orderbook_data.get('bids', [])
            asks = orderbook_data.get('asks', [])
            
            if not bids or not asks:
                return {}
            
            # Преобразуем в массивы numpy для быстрой обработки
            bid_prices = np.array([float(bid[0]) for bid in bids])
            bid_volumes = np.array([float(bid[1]) for bid in bids])
            ask_prices = np.array([float(ask[0]) for ask in asks])
            ask_volumes = np.array([float(ask[1]) for ask in asks])
            
            # Лучшие цены бид и аск
            best_bid = bid_prices[0]
            best_ask = ask_prices[0]
            
            # Средняя цена
            mid_price = (best_bid + best_ask) / 2
            
            # Группируем цены для поиска уровней
            
            # Параметры группировки
            grouping_factor = mid_price * self.price_grouping
            
            # Группировка бидов
            bid_groups = {}
            for price, volume in zip(bid_prices, bid_volumes):
                # Округляем цену до ближайшей группы
                group_price = int(price / grouping_factor) * grouping_factor
                
                if group_price in bid_groups:
                    bid_groups[group_price] += volume
                else:
                    bid_groups[group_price] = volume
            
            # Группировка асков
            ask_groups = {}
            for price, volume in zip(ask_prices, ask_volumes):
                # Округляем цену до ближайшей группы
                group_price = int(price / grouping_factor) * grouping_factor
                
                if group_price in ask_groups:
                    ask_groups[group_price] += volume
                else:
                    ask_groups[group_price] = volume
            
            # Рассчитываем средний объем для обоих сторон
            avg_bid_volume = np.mean(list(bid_groups.values()))
            avg_ask_volume = np.mean(list(ask_groups.values()))
            
            # Находим уровни поддержки (сильные уровни бидов)
            support_levels = []
            for price, volume in bid_groups.items():
                if volume > avg_bid_volume * self.volume_threshold:
                    support_levels.append({
                        'price': price,
                        'volume': volume,
                        'strength': volume / avg_bid_volume
                    })
            
            # Находим уровни сопротивления (сильные уровни асков)
            resistance_levels = []
            for price, volume in ask_groups.items():
                if volume > avg_ask_volume * self.volume_threshold:
                    resistance_levels.append({
                        'price': price,
                        'volume': volume,
                        'strength': volume / avg_ask_volume
                    })
            
            # Сортируем уровни по силе
            support_levels.sort(key=lambda x: x['strength'], reverse=True)
            resistance_levels.sort(key=lambda x: x['strength'], reverse=True)
            
            # Оставляем только top N уровней
            support_levels = support_levels[:self.max_levels]
            resistance_levels = resistance_levels[:self.max_levels]
            
            # Сортируем уровни по цене (для удобства отображения)
            support_levels.sort(key=lambda x: x['price'], reverse=True)
            resistance_levels.sort(key=lambda x: x['price'])
            
            # Сохраняем результаты
            self.results = {
                'mid_price': mid_price,
                'support_levels': support_levels,
                'resistance_levels': resistance_levels
            }
            
            return self.results
        except Exception as e:
            logger.error(f"Ошибка при анализе уровней поддержки и сопротивления: {e}")
            return {}
    
    def generate_signals(self, orderbook_data: Dict, price_data: pd.DataFrame) -> List[Signal]:
        """
        Генерация сигналов на основе анализа уровней поддержки и сопротивления
        
        Args:
            orderbook_data: Данные ордербука
            price_data: Данные о ценах
            
        Returns:
            List[Signal]: Список сгенерированных сигналов
        """
        signals = []
        
        if not self.validate_data(orderbook_data):
            return signals
        
        try:
            # Анализируем ордербук, если еще не сделано
            if not self.results:
                self.analyze(orderbook_data)
            
            # Если анализ не удался, выходим
            if not self.results:
                return signals
            
            # Получаем текущую цену закрытия
            if len(price_data) > 0:
                current_price = price_data['close'].iloc[-1]
            else:
                current_price = self.results.get('mid_price', 0)
            
            # Генерация сигналов на основе близости к уровням поддержки/сопротивления
            
            # Поиск ближайшего уровня поддержки ниже текущей цены
            nearest_support = None
            nearest_support_distance = float('inf')
            
            for level in self.results.get('support_levels', []):
                if level['price'] < current_price:
                    distance = current_price - level['price']
                    if distance < nearest_support_distance:
                        nearest_support = level
                        nearest_support_distance = distance
            
            # Поиск ближайшего уровня сопротивления выше текущей цены
            nearest_resistance = None
            nearest_resistance_distance = float('inf')
            
            for level in self.results.get('resistance_levels', []):
                if level['price'] > current_price:
                    distance = level['price'] - current_price
                    if distance < nearest_resistance_distance:
                        nearest_resistance = level
                        nearest_resistance_distance = distance
            
            # Вычисляем относительное расстояние до уровней
            if nearest_support:
                support_distance_percent = nearest_support_distance / current_price * 100
            else:
                support_distance_percent = float('inf')
            
            if nearest_resistance:
                resistance_distance_percent = nearest_resistance_distance / current_price * 100
            else:
                resistance_distance_percent = float('inf')
            
            # Генерация сигналов
            
            # Если цена близко к сильному уровню поддержки
            if nearest_support and support_distance_percent <= 1.0:  # В пределах 1%
                # Сила сигнала зависит от расстояния и силы уровня
                strength = min(0.5 + (nearest_support['strength'] / 5) * (1 - support_distance_percent), 0.9)
                
                signal = Signal(
                    type=SignalType.BUY,
                    strength=strength,
                    indicator="Support_Level",
                    timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                    timeframe="",
                    price=current_price,
                    description=f"Цена приближается к сильному уровню поддержки {nearest_support['price']:.8f} (сила: {nearest_support['strength']:.2f}x)",
                    stop_loss=nearest_support['price'] * 0.99,  # Стоп-лосс немного ниже уровня поддержки
                    take_profit=current_price * 1.03  # Тейк-профит на 3% выше текущей цены
                )
                
                signals.append(signal)
            
            # Если цена близко к сильному уровню сопротивления
            if nearest_resistance and resistance_distance_percent <= 1.0:  # В пределах 1%
                # Сила сигнала зависит от расстояния и силы уровня
                strength = min(0.5 + (nearest_resistance['strength'] / 5) * (1 - resistance_distance_percent), 0.9)
                
                signal = Signal(
                    type=SignalType.SELL,
                    strength=strength,
                    indicator="Resistance_Level",
                    timestamp=datetime.now() if not isinstance(price_data.index, pd.DatetimeIndex) else price_data.index[-1],
                    timeframe="",
                    price=current_price,
                    description=f"Цена приближается к сильному уровню сопротивления {nearest_resistance['price']:.8f} (сила: {nearest_resistance['strength']:.2f}x)",
                    stop_loss=nearest_resistance['price'] * 1.01,  # Стоп-лосс немного выше уровня сопротивления
                    take_profit=current_price * 0.97  # Тейк-профит на 3% ниже текущей цены
                )
                
                signals.append(signal)
            
            return signals
        except Exception as e:
            logger.error(f"Ошибка при генерации сигналов на основе анализа уровней поддержки и сопротивления: {e}")
            return []

# ===========================================================================
# БЛОК 11: ОСНОВНОЙ КЛАСС INDICATOR ENGINE
# ===========================================================================

class IndicatorEngine:
    """
    Основной класс для обработки индикаторов и генерации сигналов
    """
    
    def __init__(self, test_mode: bool = False, test_symbols: List[str] = None):
        """
        Инициализация IndicatorEngine
        
        Args:
            test_mode: Режим тестирования
            test_symbols: Список символов для тестирования
        """
        self.start_time = datetime.now()
        self.test_mode = test_mode
        self.test_symbols = test_symbols or ["BTC", "ETH", "XRP", "SOL", "BNB"]
        
        # Инициализация логгера
        self.logger = get_logger('indicator_engine', 'indicators')
        
        # Создание директорий
        self._setup_directories()
        
        # Настройка параметров обработки
        self._load_config()
        
        # Инициализация менеджера потоков для параллельной обработки
        self.thread_manager = ThreadManager(
            max_workers=self.processing_config.get('max_workers', 24),
            dynamic_workers=self.processing_config.get('dynamic_worker_adjustment', True)
        )
        
        # Инициализация фабрик для индикаторов и паттернов
        self.indicator_factory = IndicatorFactory()
        self.pattern_factory = PatternFactory()
        self.divergence_factory = DivergenceFactory()
        self.orderbook_analyzer_factory = OrderbookAnalyzerFactory()
        
        # Создание индикаторов и детекторов по конфигурации
        self._create_analyzers()
        
        # Статистика обработки
        self.stats = {
            'total_symbols': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_signals': 0,
            'top_symbols': [],
            'execution_time': 0
        }
        
        self.logger.info(f"IndicatorEngine инициализирован. Режим тестирования: {self.test_mode}")
    
    def _setup_directories(self):
        """Настройка необходимых директорий"""
        # Получение путей из конфигурации
        if 'PATHS' in globals():
            self.paths = PATHS
        else:
            # Базовые пути по умолчанию
            self.paths = {
                'BRAIN_DATA': "data/brain/",
                'BRAIN_LOGS': "logs/brain/",
                'DATA_ENGINE': {
                    'current': "data/brain/data_engine/current/",
                    'archive': "data/brain/data_engine/archive/",
                    'temp': "data/brain/data_engine/temp/"
                },
                'INDICATORS': {
                    'current': "data/brain/indicators/current/",
                    'archive': "data/brain/indicators/archive/",
                    'temp': "data/brain/indicators/temp/"
                },
                'SIGNALS': {
                    'current': "data/brain/signals/current/",
                    'archive': "data/brain/signals/archive/",
                    'temp': "data/brain/signals/temp/"
                }
            }
        
        # Создание необходимых директорий
        for path_type in ['INDICATORS', 'SIGNALS']:
            for component in ['current', 'archive', 'temp']:
                if path_type in self.paths and component in self.paths[path_type]:
                    dir_path = os.path.join(ROOT_DIR, self.paths[path_type][component])
                    os.makedirs(dir_path, exist_ok=True)
                    self.logger.debug(f"Создана директория: {dir_path}")
    
    def _load_config(self):
        """Загрузка конфигурации"""
        # Настройки таймфреймов
        if 'TIMEFRAMES' in globals():
            self.timeframes_config = TIMEFRAMES
        else:
            # Настройки таймфреймов по умолчанию
            self.timeframes_config = {
                '7d': {'enabled': True, 'weight': 0.1},    # 10%
                '24h': {'enabled': True, 'weight': 0.1},   # 10%
                '4h': {'enabled': True, 'weight': 0.25},   # 25%
                '1h': {'enabled': True, 'weight': 0.25},   # 25%
                '30m': {'enabled': True, 'weight': 0.1},   # 10%
                '15m': {'enabled': True, 'weight': 0.1},   # 10%
                '5m': {'enabled': True, 'weight': 0.1}     # 10%
            }
        
        # Настройки индикаторов
        if 'INDICATORS' in globals():
            self.indicators_config = INDICATORS
        else:
            # Настройки индикаторов по умолчанию
            self.indicators_config = {
                'phase1': {
                    'enabled': True,
                    'trend': {
                        'MACD': {'enabled': True},
                        'EMA': {'enabled': True}
                    },
                    'momentum': {
                        'RSI': {'enabled': True}
                    },
                    'volatility': {
                        'ATR': {'enabled': True}
                    }
                }
            }
        
        # Настройки обработки
        if 'PROCESSING' in globals():
            self.processing_config = PROCESSING
        else:
            # Настройки обработки по умолчанию
            self.processing_config = {
                'batch_size': 40,
                'max_workers': 24,
                'dynamic_worker_adjustment': True,
                'retry_attempts': 2,
                'retry_delay': 2
            }
    
    def _create_analyzers(self):
        """Создание индикаторов и детекторов по конфигурации"""
        self.indicators = {}
        self.pattern_detectors = {}
        self.divergence_detectors = {}
        self.orderbook_analyzers = {}
        
        # Создание индикаторов на основе конфигурации
        for phase, phase_config in self.indicators_config.items():
            if phase_config.get('enabled', False):
                # Трендовые индикаторы
                for indicator_name, indicator_config in phase_config.get('trend', {}).items():
                    if indicator_config.get('enabled', False):
                        self.indicators[indicator_name] = self._get_indicator_instance('trend', indicator_name, indicator_config)
                
                # Индикаторы моментума
                for indicator_name, indicator_config in phase_config.get('momentum', {}).items():
                    if indicator_config.get('enabled', False):
                        self.indicators[indicator_name] = self._get_indicator_instance('momentum', indicator_name, indicator_config)
                
                # Индикаторы волатильности
                for indicator_name, indicator_config in phase_config.get('volatility', {}).items():
                    if indicator_config.get('enabled', False):
                        self.indicators[indicator_name] = self._get_indicator_instance('volatility', indicator_name, indicator_config)
                
                # Объемные индикаторы
                for indicator_name, indicator_config in phase_config.get('volume', {}).items():
                    if indicator_config.get('enabled', False):
                        self.indicators[indicator_name] = self._get_indicator_instance('volume', indicator_name, indicator_config)
        
        # Создание детекторов паттернов
        self.pattern_detectors['price'] = PricePatternDetector()
        self.pattern_detectors['volume'] = VolumePatternDetector()
        self.pattern_detectors['volatility'] = VolatilityPatternDetector()
        self.pattern_detectors['combined'] = CombinedPatternDetector()
        
        # Создание детекторов дивергенций
        self.divergence_detectors['RSI'] = RSIDivergenceDetector()
        self.divergence_detectors['MACD'] = MACDDivergenceDetector()
        self.divergence_detectors['Stochastic'] = StochasticDivergenceDetector()
        self.divergence_detectors['OBV'] = OBVDivergenceDetector()
        
        # Создание анализаторов ордербуков
        self.orderbook_analyzers['liquidity'] = LiquidityAnalyzer()
        self.orderbook_analyzers['imbalance'] = ImbalanceAnalyzer()
        self.orderbook_analyzers['support_resistance'] = SupportResistanceAnalyzer()
        
        self.logger.info(f"Создано {len(self.indicators)} индикаторов, {len(self.pattern_detectors)} детекторов паттернов, "
                        f"{len(self.divergence_detectors)} детекторов дивергенций, {len(self.orderbook_analyzers)} анализаторов ордербуков")
    
    def _get_indicator_instance(self, indicator_type: str, indicator_name: str, indicator_config: Dict) -> BaseIndicator:
        """
        Получение экземпляра индикатора на основе типа и имени
        
        Args:
            indicator_type: Тип индикатора
            indicator_name: Имя индикатора
            indicator_config: Конфигурация индикатора
            
        Returns:
            BaseIndicator: Экземпляр индикатора
        """
        # Проверка встроенных классов
        indicator_map = {
            'MACD': MACDIndicator,
            'EMA': EMAIndicator,
            'ADX': ADXIndicator,
            'RSI': RSIIndicator,
            'Stochastic': StochasticIndicator,
            'ATR': ATRIndicator,
            'Bollinger': BollingerBandsIndicator,
            'OBV': OBVIndicator,
            'Delta': DeltaVolumeIndicator,
            'VSA': VSAIndicator
        }
        
        if indicator_name in indicator_map:
            return indicator_map[indicator_name](indicator_config)
        
        # Если не найден во встроенных, используем фабрику
        return self.indicator_factory.create(indicator_type, indicator_name, indicator_config)
    
    def get_enabled_timeframes(self) -> List[str]:
        """
        Получение списка активных таймфреймов
        
        Returns:
            List[str]: Список активных таймфреймов
        """
        return [tf for tf, config in self.timeframes_config.items() if config.get('enabled', True)]
    
    def get_symbols(self) -> List[str]:
        """
        Получение списка символов для обработки
        
        Returns:
            List[str]: Список символов
        """
        if self.test_mode:
            return self.test_symbols
        
        symbols = []
        
        try:
            # Получаем список символов из директории с историческими данными
            timeframes = self.get_enabled_timeframes()
            
            if not timeframes:
                self.logger.error("Не найдены активные таймфреймы")
                return symbols
            
            # Используем первый активный таймфрейм для поиска доступных символов
            base_timeframe = timeframes[0]
            timeframe_dir = os.path.join(ROOT_DIR, self.paths.get('DATA_ENGINE', {}).get('current', ''), base_timeframe)
            
            if os.path.exists(timeframe_dir):
                for file in os.listdir(timeframe_dir):
                    if file.endswith(".json"):
                        symbol = file.replace(".json", "")
                        symbols.append(symbol)
                
                self.logger.info(f"Найдено {len(symbols)} символов для обработки")
            else:
                self.logger.error(f"Директория {timeframe_dir} не существует")
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка символов: {e}")
        
        return symbols
    
    def load_market_data(self, symbol: str, timeframe: str) -> Dict:
        """
        Загрузка исторических рыночных данных для символа и таймфрейма
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            
        Returns:
            Dict: Исторические данные
        """
        try:
            # Формируем путь к файлу
            data_path = os.path.join(ROOT_DIR, self.paths.get('DATA_ENGINE', {}).get('current', ''), timeframe, f"{symbol}.json")
            
            # Проверяем наличие файла
            if not os.path.exists(data_path):
                self.logger.warning(f"Файл данных не найден: {data_path}")
                return None
            
            # Загружаем данные
            with open(data_path, 'r') as f:
                data = json.load(f)
            
            return data
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных {symbol} ({timeframe}): {e}")
            return None
    
    def load_orderbook_data(self, symbol: str) -> Dict:
        """
        Загрузка данных ордербука для символа
        
        Args:
            symbol: Символ монеты
            
        Returns:
            Dict: Данные ордербука
        """
        try:
            # Формируем путь к файлу ордербука
            # Обычно они хранятся в отдельной директории или с особым маркером в имени файла
            orderbook_path = os.path.join(ROOT_DIR, self.paths.get('DATA_ENGINE', {}).get('current', ''), "orderbooks", f"{symbol}_orderbook.json")
            
            # Проверяем наличие файла
            if not os.path.exists(orderbook_path):
                self.logger.debug(f"Файл ордербука не найден: {orderbook_path}")
                return None
            
            # Загружаем данные
            with open(orderbook_path, 'r') as f:
                data = json.load(f)
            
            return data
        except Exception as e:
            self.logger.debug(f"Ошибка при загрузке ордербука {symbol}: {e}")
            return None
    
    def preprocess_data(self, market_data: Dict) -> pd.DataFrame:
        """
        Предобработка рыночных данных и преобразование в DataFrame
        
        Args:
            market_data: Исторические данные в формате словаря
            
        Returns:
            pd.DataFrame: Предобработанные данные
        """
        if not market_data:
            return pd.DataFrame()
        
        try:
            # Проверка структуры данных
            if isinstance(market_data, list):
                # Данные уже в формате списка словарей
                data_list = market_data
            elif isinstance(market_data, dict) and 'data' in market_data:
                # Данные в формате {'data': [...]}
                data_list = market_data['data']
            else:
                self.logger.error(f"Неизвестный формат данных: {type(market_data)}")
                return pd.DataFrame()
            
            # Извлекаем OHLCV данные из структуры
            ohlcv_data = []
            
            for item in data_list:
                timestamp = item.get('time_close', item.get('timestamp', ''))
                
                # Проверяем формат данных (CoinMarketCap или другой)
                if 'quote' in item and 'USD' in item['quote']:
                    # Формат CoinMarketCap
                    quote = item['quote']['USD']
                    
                    if 'open' not in quote or 'high' not in quote or 'low' not in quote or 'close' not in quote or 'volume' not in quote:
                        continue
                    
                    ohlcv_data.append({
                        'timestamp': timestamp,
                        'open': float(quote['open']),
                        'high': float(quote['high']),
                        'low': float(quote['low']),
                        'close': float(quote['close']),
                        'volume': float(quote['volume'])
                    })
                elif all(k in item for k in ['open', 'high', 'low', 'close', 'volume']):
                    # Стандартный формат OHLCV
                    ohlcv_data.append({
                        'timestamp': timestamp,
                        'open': float(item['open']),
                        'high': float(item['high']),
                        'low': float(item['low']),
                        'close': float(item['close']),
                        'volume': float(item['volume'])
                    })
            
            if not ohlcv_data:
                self.logger.warning(f"Не удалось извлечь OHLCV данные из маркет данных")
                return pd.DataFrame()
            
            # Создаем DataFrame
            df = pd.DataFrame(ohlcv_data)
            
            # Преобразуем timestamp в datetime и устанавливаем как индекс
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            df = df.set_index('timestamp')
            
            # Удаляем строки с NaN
            df = df.dropna()
            
            return df
        except Exception as e:
            self.logger.error(f"Ошибка при предобработке данных: {e}")
            return pd.DataFrame()
    
    def calculate_all_indicators(self, df: pd.DataFrame, timeframe: str) -> Dict:
        """
        Расчет всех индикаторов для DataFrame
        
        Args:
            df: DataFrame с OHLCV данными
            timeframe: Таймфрейм
            
        Returns:
            Dict: Словарь с результатами расчета всех индикаторов
        """
        indicators_data = {}
        
        if df.empty:
            return indicators_data
        
        try:
            # Определяем фазу рынка
            market_phase = get_market_phase(df)
            
            # Расчет всех активных индикаторов
            for name, indicator in self.indicators.items():
                # Настраиваем параметры для конкретного таймфрейма
                indicator.params['timeframe'] = timeframe
                
                # Расчет индикатора
                result = indicator.calculate(df)
                
                # Сохраняем результат
                if result:
                    indicators_data[name] = result
                    
                    # Генерируем сигналы
                    indicator.generate_signals(df)
            
            return indicators_data
        except Exception as e:
            self.logger.error(f"Ошибка при расчете индикаторов: {e}")
            return {}
    
    def detect_patterns(self, df: pd.DataFrame, timeframe: str) -> Dict:
        """
        Обнаружение паттернов
        
        Args:
            df: DataFrame с OHLCV данными
            timeframe: Таймфрейм
            
        Returns:
            Dict: Словарь с обнаруженными паттернами
        """
        patterns_data = {}
        
        if df.empty:
            return patterns_data
        
        try:
            # Обнаружение паттернов с помощью всех детекторов
            for name, detector in self.pattern_detectors.items():
                patterns = detector.detect(df)
                
                if patterns:
                    patterns_data[name] = [
                        {
                            'type': pattern.type.value,
                            'subtype': pattern.subtype,
                            'start_idx': pattern.start_idx,
                            'end_idx': pattern.end_idx,
                            'confidence': pattern.confidence,
                            'expected_direction': pattern.expected_direction.value,
                            'description': pattern.description,
                            'support_levels': pattern.support_levels,
                            'resistance_levels': pattern.resistance_levels,
                            'price_targets': pattern.price_targets
                        }
                        for pattern in patterns
                    ]
            
            return patterns_data
        except Exception as e:
            self.logger.error(f"Ошибка при обнаружении паттернов: {e}")
            return {}
    
    def detect_divergences(self, df: pd.DataFrame, indicators_data: Dict, timeframe: str) -> Dict:
        """
        Обнаружение дивергенций
        
        Args:
            df: DataFrame с OHLCV данными
            indicators_data: Данные индикаторов
            timeframe: Таймфрейм
            
        Returns:
            Dict: Словарь с обнаруженными дивергенциями
        """
        divergences_data = {}
        
        if df.empty or not indicators_data:
            return divergences_data
        
        try:
            # Обнаружение дивергенций с помощью всех детекторов
            for name, detector in self.divergence_detectors.items():
                # Проверяем, есть ли необходимые индикаторы
                required_indicators = detector.required_indicators
                
                if all(ind in indicators_data for ind in required_indicators):
                    divergences = detector.detect(df, indicators_data)
                    
                    if divergences:
                        divergences_data[name] = [
                            {
                                'type': divergence.type.value,
                                'price_point1': divergence.price_point1,
                                'price_point2': divergence.price_point2,
                                'indicator_point1': divergence.indicator_point1,
                                'indicator_point2': divergence.indicator_point2,
                                'indicator_name': divergence.indicator_name,
                                'strength': divergence.strength,
                                'confirmed': divergence.confirmed,
                                'description': divergence.description
                            }
                            for divergence in divergences
                        ]
            
            return divergences_data
        except Exception as e:
            self.logger.error(f"Ошибка при обнаружении дивергенций: {e}")
            return {}
    
    def analyze_orderbook(self, orderbook_data: Dict, price_data: pd.DataFrame) -> Dict:
        """
        Анализ ордербука
        
        Args:
            orderbook_data: Данные ордербука
            price_data: Данные о ценах
            
        Returns:
            Dict: Результаты анализа ордербука
        """
        orderbook_analysis = {}
        
        if not orderbook_data:
            return orderbook_analysis
        
        try:
            # Анализ ордербука с помощью всех анализаторов
            for name, analyzer in self.orderbook_analyzers.items():
                result = analyzer.analyze(orderbook_data)
                
                if result:
                    orderbook_analysis[name] = result
            
            return orderbook_analysis
        except Exception as e:
            self.logger.error(f"Ошибка при анализе ордербука: {e}")
            return {}
    
    def collect_signals(self, df: pd.DataFrame, timeframe: str) -> List[Dict]:
        """
        Сбор сигналов от всех индикаторов и детекторов
        
        Args:
            df: DataFrame с OHLCV данными
            timeframe: Таймфрейм
            
        Returns:
            List[Dict]: Список сигналов
        """
        signals = []
        
        # Сбор сигналов от индикаторов
        for name, indicator in self.indicators.items():
            for signal in indicator.signals:
                signals.append({
                    'type': signal.type.value,
                    'strength': signal.strength,
                    'indicator': signal.indicator,
                    'timestamp': signal.timestamp.isoformat() if signal.timestamp else None,
                    'timeframe': timeframe,
                    'price': signal.price,
                    'description': signal.description,
                    'stop_loss': signal.stop_loss,
                    'take_profit': signal.take_profit
                })
        
        return signals
    
    def collect_orderbook_signals(self, orderbook_data: Dict, price_data: pd.DataFrame) -> List[Dict]:
        """
        Сбор сигналов от анализаторов ордербуков
        
        Args:
            orderbook_data: Данные ордербука
            price_data: Данные о ценах
            
        Returns:
            List[Dict]: Список сигналов
        """
        signals = []
        
        if not orderbook_data:
            return signals
        
        # Сбор сигналов от анализаторов ордербуков
        for name, analyzer in self.orderbook_analyzers.items():
            analyzer_signals = analyzer.generate_signals(orderbook_data, price_data)
            
            for signal in analyzer_signals:
                signals.append({
                    'type': signal.type.value,
                    'strength': signal.strength,
                    'indicator': signal.indicator,
                    'timestamp': signal.timestamp.isoformat() if signal.timestamp else None,
                    'timeframe': signal.timeframe,
                    'price': signal.price,
                    'description': signal.description,
                    'stop_loss': signal.stop_loss,
                    'take_profit': signal.take_profit
                })
        
        return signals
    
    def save_analysis_results(self, symbol: str, timeframe: str, data: Dict) -> bool:
        """
        Сохранение результатов анализа
        
        Args:
            symbol: Символ монеты
            timeframe: Таймфрейм
            data: Данные для сохранения
            
        Returns:
            bool: True если сохранение успешно
        """
        try:
            # Формируем путь к директории
            results_dir = os.path.join(ROOT_DIR, self.paths.get('INDICATORS', {}).get('current', ''), timeframe)
            os.makedirs(results_dir, exist_ok=True)
            
            # Формируем путь к файлу
            results_path = os.path.join(results_dir, f"{symbol}_analysis.json")
            
            # Сохраняем результаты
            with open(results_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении результатов анализа {symbol} ({timeframe}): {e}")
            return False
    
    def save_signals(self, symbol: str, signals: List[Dict]) -> bool:
        """
        Сохранение сигналов
        
        Args:
            symbol: Символ монеты
            signals: Список сигналов
            
        Returns:
            bool: True если сохранение успешно
        """
        try:
            # Формируем путь к директории
            signals_dir = os.path.join(ROOT_DIR, self.paths.get('SIGNALS', {}).get('current', ''))
            os.makedirs(signals_dir, exist_ok=True)
            
            # Формируем путь к файлу
            signals_path = os.path.join(signals_dir, f"{symbol}_signals.json")
            
            # Сохраняем сигналы
            with open(signals_path, 'w') as f:
                json.dump(signals, f, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении сигналов {symbol}: {e}")
            return False
    
    def analyze_symbol(self, symbol: str, timeframes: List[str]) -> Dict:
        """
        Анализ одного символа по всем таймфреймам
        
        Args:
            symbol: Символ монеты
            timeframes: Список таймфреймов
            
        Returns:
            Dict: Результаты анализа
        """
        start_time = time.time()
        self.logger.info(f"Начало анализа символа {symbol}")
        
        results = {
            'symbol': symbol,
            'timeframes': {},
            'signals': [],
            'success': False
        }
        
        try:
            all_signals = []
            
            # Анализ по всем таймфреймам
            for timeframe in timeframes:
                timeframe_start = time.time()
                
                # Загрузка данных
                market_data = self.load_market_data(symbol, timeframe)
                
                if not market_data:
                    results['timeframes'][timeframe] = {
                        'success': False,
                        'message': "Не удалось загрузить рыночные данные"
                    }
                    continue
                
                # Предобработка данных
                df = self.preprocess_data(market_data)
                
                if df.empty:
                    results['timeframes'][timeframe] = {
                        'success': False,
                        'message': "Не удалось обработать рыночные данные"
                    }
                    continue
                
                # Расчет индикаторов
                indicators_data = self.calculate_all_indicators(df, timeframe)
                
                # Обнаружение паттернов
                patterns_data = self.detect_patterns(df, timeframe)
                
                # Обнаружение дивергенций
                divergences_data = self.detect_divergences(df, indicators_data, timeframe)
                
                # Загрузка и анализ ордербука (только для последнего таймфрейма)
                orderbook_data = None
                orderbook_analysis = {}
                orderbook_signals = []
                
                if timeframe == timeframes[-1]:
                    orderbook_data = self.load_orderbook_data(symbol)
                    
                    if orderbook_data:
                        orderbook_analysis = self.analyze_orderbook(orderbook_data, df)
                        orderbook_signals = self.collect_orderbook_signals(orderbook_data, df)
                
                # Сбор сигналов
                timeframe_signals = self.collect_signals(df, timeframe)
                all_signals.extend(timeframe_signals)
                all_signals.extend(orderbook_signals)
                
                # Сохранение результатов для таймфрейма
                analysis_data = {
                    'indicators': indicators_data,
                    'patterns': patterns_data,
                    'divergences': divergences_data,
                    'orderbook': orderbook_analysis,
                    'signals': timeframe_signals + orderbook_signals
                }
                
                save_success = self.save_analysis_results(symbol, timeframe, analysis_data)
                
                # Запись результатов таймфрейма
                results['timeframes'][timeframe] = {
                    'success': save_success,
                    'data_points': len(df),
                    'signals_count': len(timeframe_signals) + len(orderbook_signals),
                    'execution_time': time.time() - timeframe_start
                }
                
                self.logger.info(f"Анализ {symbol} ({timeframe}) завершен за {time.time() - timeframe_start:.2f} сек")
            
            # Сохранение всех сигналов
            save_signals_success = self.save_signals(symbol, all_signals)
            
            # Запись всех сигналов в результаты
            results['signals'] = all_signals
            results['success'] = save_signals_success
            results['execution_time'] = time.time() - start_time
            
            self.logger.info(f"Анализ символа {symbol} завершен за {time.time() - start_time:.2f} сек")
            
            return results
        except Exception as e:
            self.logger.error(f"Ошибка при анализе символа {symbol}: {e}")
            results['error'] = str(e)
            results['execution_time'] = time.time() - start_time
            return results
    
    def analyze_symbol_batch(self, symbols: List[str], timeframes: List[str]) -> List[Dict]:
        """
        Анализ пакета символов
        
        Args:
            symbols: Список символов
            timeframes: Список таймфреймов
            
        Returns:
            List[Dict]: Результаты анализа
        """
        results = []
        
        for symbol in symbols:
            result = self.analyze_symbol(symbol, timeframes)
            results.append(result)
        
        return results
    
    async def analyze_symbol_async(self, symbol: str, timeframes: List[str]) -> Dict:
        """
        Асинхронный анализ одного символа
        
        Args:
            symbol: Символ монеты
            timeframes: Список таймфреймов
            
        Returns:
            Dict: Результаты анализа
        """
        # Для CPU-bound операций используем ThreadPoolExecutor
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.analyze_symbol, symbol, timeframes)
    
    async def analyze_symbols_async(self, symbols: List[str], timeframes: List[str]) -> List[Dict]:
        """
        Асинхронный анализ нескольких символов
        
        Args:
            symbols: Список символов
            timeframes: Список таймфреймов
            
        Returns:
            List[Dict]: Результаты анализа
        """
        # Создаем задачи для асинхронного анализа
        tasks = []
        for symbol in symbols:
            tasks.append(self.analyze_symbol_async(symbol, timeframes))
        
        # Выполняем задачи
        return await asyncio.gather(*tasks)
    
    def rank_symbols(self, results: List[Dict]) -> List[Dict]:
        """
        Ранжирование символов на основе результатов анализа
        
        Args:
            results: Результаты анализа
            
        Returns:
            List[Dict]: Ранжированный список символов
        """
        ranked_symbols = []
        
        for result in results:
            if not result.get('success', False):
                continue
            
            symbol = result['symbol']
            signals = result.get('signals', [])
            
            # Подсчет сигналов по типам
            buy_signals = [s for s in signals if s['type'] in ['buy', 'strong_buy']]
            sell_signals = [s for s in signals if s['type'] in ['sell', 'strong_sell']]
            
            # Рассчитываем общий скор
            buy_score = sum(s['strength'] for s in buy_signals)
            sell_score = sum(s['strength'] for s in sell_signals)
            
            # Определяем общее направление
            if buy_score > sell_score:
                direction = 'buy'
                score = buy_score - sell_score
            elif sell_score > buy_score:
                direction = 'sell'
                score = sell_score - buy_score
            else:
                direction = 'neutral'
                score = 0
            
            # Добавляем в список
            ranked_symbols.append({
                'symbol': symbol,
                'direction': direction,
                'score': score,
                'buy_signals': len(buy_signals),
                'sell_signals': len(sell_signals),
                'buy_score': buy_score,
                'sell_score': sell_score,
                'top_signals': sorted(signals, key=lambda s: s['strength'], reverse=True)[:5]
            })
        
        # Сортируем по скору
        ranked_symbols.sort(key=lambda x: x['score'], reverse=True)
        
        return ranked_symbols
    
    def run(self) -> Dict:
        """
        Запуск обработки индикаторов
        
        Returns:
            Dict: Результаты обработки
        """
        run_start_time = time.time()
        
        self.logger.info("Запуск обработки индикаторов")
        
        try:
            # Получаем символы для обработки
            symbols = self.get_symbols()
            
            if not symbols:
                error_msg = "Не найдены монеты для обработки"
                self.logger.error(error_msg)
                return {
                    "success": False,
                    "message": error_msg
                }
            
            # Получаем активные таймфреймы
            timeframes = self.get_enabled_timeframes()
            
            if not timeframes:
                error_msg = "Не найдены активные таймфреймы"
                self.logger.error(error_msg)
                return {
                    "success": False,
                    "message": error_msg
                }
            
            self.logger.info(f"Начало обработки {len(symbols)} монет по {len(timeframes)} таймфреймам")
            
            # Распределение задач по пакетам
            batch_size = self.processing_config.get('batch_size', 40)
            results = self.thread_manager.distribute_tasks(
                symbols, self.analyze_symbol, batch_size=batch_size, timeframes=timeframes
            )
            
            # Подсчет статистики
            successful_results = [r for r in results if r.get('success', False)]
            
            # Обновляем статистику
            self.stats['total_symbols'] = len(symbols)
            self.stats['successful_symbols'] = len(successful_results)
            self.stats['failed_symbols'] = len(symbols) - len(successful_results)
            self.stats['total_signals'] = sum(len(r.get('signals', [])) for r in results)
            self.stats['execution_time'] = time.time() - run_start_time
            
            # Ранжирование символов
            ranked_symbols = self.rank_symbols(results)
            self.stats['top_symbols'] = ranked_symbols[:20]  # Top 20 символов
            
            # Сохранение топ символов
            self._save_top_symbols(ranked_symbols)
            
            self.logger.info(f"Обработка индикаторов завершена за {self.stats['execution_time']:.2f} сек. " +
                           f"Успешно: {self.stats['successful_symbols']}/{self.stats['total_symbols']} монет")
            
            return {
                "success": self.stats['successful_symbols'] > 0,
                "total_symbols": self.stats['total_symbols'],
                "successful_symbols": self.stats['successful_symbols'],
                "failed_symbols": self.stats['failed_symbols'],
                "total_signals": self.stats['total_signals'],
                "execution_time": self.stats['execution_time'],
                "top_symbols": [s['symbol'] for s in self.stats['top_symbols']]
            }
        except Exception as e:
            total_time = time.time() - run_start_time
            self.logger.error(f"Ошибка при обработке индикаторов: {e}")
            
            return {
                "success": False,
                "error": str(e),
                "execution_time": total_time
            }
    
    def _save_top_symbols(self, ranked_symbols: List[Dict]) -> bool:
        """
        Сохранение топ символов
        
        Args:
            ranked_symbols: Ранжированный список символов
            
        Returns:
            bool: True если сохранение успешно
        """
        try:
            # Формируем путь к директории
            signals_dir = os.path.join(ROOT_DIR, self.paths.get('SIGNALS', {}).get('current', ''))
            os.makedirs(signals_dir, exist_ok=True)
            
            # Формируем путь к файлу
            top_symbols_path = os.path.join(signals_dir, "top_symbols.json")
            
            # Фильтруем и сохраняем только монеты с положительным скором
            buy_symbols = [s for s in ranked_symbols if s['direction'] == 'buy' and s['score'] > 0]
            sell_symbols = [s for s in ranked_symbols if s['direction'] == 'sell' and s['score'] > 0]
            
            top_symbols_data = {
                'timestamp': datetime.now().isoformat(),
                'buy': buy_symbols[:20],  # Top 20 монет для покупки
                'sell': sell_symbols[:20]  # Top 20 монет для продажи
            }
            
            # Сохраняем результаты
            with open(top_symbols_path, 'w') as f:
                json.dump(top_symbols_data, f, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении топ символов: {e}")
            return False

# ===========================================================================
# БЛОК 12: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И ТОЧКА ВХОДА
# ===========================================================================

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='IndicatorEngine - модуль расчета индикаторов и сигналов')
    parser.add_argument('--test', action='store_true', help='Запуск в тестовом режиме')
    parser.add_argument('--symbols', nargs='+', help='Список символов для обработки в тестовом режиме')
    parser.add_argument('--timeframes', nargs='+', help='Список таймфреймов для обработки')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
                        default='INFO', help='Уровень логирования')
    return parser.parse_args()

def main():
    """Основная функция"""
    # Парсинг аргументов
    args = parse_args()
    
    # Настройка уровня логирования
    logging_level = getattr(logging, args.log_level)
    logging.basicConfig(level=logging_level)
    
    # Инициализация IndicatorEngine
    test_mode = args.test
    test_symbols = args.symbols if args.symbols else ["BTC", "ETH", "XRP", "SOL", "BNB"]
    
    # Вывод информации о запуске
    logger.info(f"Запуск IndicatorEngine. Тестовый режим: {test_mode}")
    if test_mode:
        logger.info(f"Тестовые символы: {test_symbols}")
    
    # Запуск IndicatorEngine
    engine = IndicatorEngine(test_mode=test_mode, test_symbols=test_symbols)
    result = engine.run()
    
    # Вывод результатов
    logger.info(f"Обработка завершена. Результат: {result}")
    
    # Если указаны конкретные временные рамки, выводим дополнительную статистику
    if args.timeframes:
        timeframe_stats = {tf: {'count': 0, 'success': 0, 'failed': 0, 'avg_time': 0} for tf in args.timeframes}
        
        for symbol_result in result.get('results', []):
            for tf, tf_result in symbol_result.get('timeframes', {}).items():
                if tf in timeframe_stats:
                    timeframe_stats[tf]['count'] += 1
                    
                    if tf_result.get('success', False):
                        timeframe_stats[tf]['success'] += 1
                    else:
                        timeframe_stats[tf]['failed'] += 1
                    
                    timeframe_stats[tf]['avg_time'] += tf_result.get('execution_time', 0)
        
        # Рассчитываем среднее время выполнения
        for tf in timeframe_stats:
            if timeframe_stats[tf]['count'] > 0:
                timeframe_stats[tf]['avg_time'] /= timeframe_stats[tf]['count']
        
        logger.info(f"Статистика по таймфреймам: {timeframe_stats}")
    
    # Выводим топ символы
    for direction in ['buy', 'sell']:
        top_symbols = [s for s in engine.stats.get('top_symbols', []) if s['direction'] == direction][:5]
        
        if top_symbols:
            logger.info(f"Топ 5 символов для {direction}: {[s['symbol'] for s in top_symbols]}")
    
    # Отправка уведомления в Telegram
    try:
        if LOGGER_IMPORTED:
            execution_time = result.get('execution_time', 0)
            successful = result.get('successful_symbols', 0)
            total = result.get('total_symbols', 0)
            
            status = "успешно" if result.get('success', False) else "с ошибками"
            
            message = (
                f"📊 Обработка индикаторов завершена {status}\n"
                f"⏱️ Время выполнения: {execution_time:.2f} сек\n"
                f"📈 Обработано: {successful}/{total} монет\n"
            )
            
            if result.get('top_symbols'):
                message += f"🔝 Топ символы: {', '.join(result.get('top_symbols')[:5])}\n"
            
            send_telegram(message)
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления в Telegram: {e}")
    
    return result

if __name__ == "__main__":
    main()
