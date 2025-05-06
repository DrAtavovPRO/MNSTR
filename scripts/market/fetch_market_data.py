# ============= BLOCK 1: CONFIGURATION AND INITIALIZATION =============

# [RECONSTRUCTION 1.1] Base imports - сохраняем все импорты для будущего использования
import requests
import json
import os
import gzip
import statistics
from datetime import datetime
from dotenv import load_dotenv
import logging
import shutil
import math
import numpy as np
from typing import Dict, List, Optional, Any

# [RECONSTRUCTION 1.2] Environment configuration
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', '.env')
load_dotenv(env_path)
CMC_API_KEY = os.getenv("CMC_API_KEY")

# [RECONSTRUCTION 1.3] Path configuration
PATHS = {
    'MARKET_CURRENT': "data/market/current/",
    'MARKET_ARCHIVE': "data/market/archive/",
    'LOGS': "logs/market/",
    'TEMP': "data/market/temp/"
}

# [RECONSTRUCTION 1.4] Updated base configuration
CONFIG = {
    'VOLUME_THRESHOLD': 500000,      # Обновленный минимальный порог
    'MIN_PRICE': 0.00001,           # Минимальная цена монеты
    'MAX_COINS': 2000,              # Максимальное количество монет для анализа
    'REQUEST_TIMEOUT': 30,          # Таймаут API запросов
    'KEEP_RECENT_FILES': 5,         # Количество сохраняемых файлов
    'ANALYSIS_TIMEFRAMES': ['1h', '24h', '7d']  # Основные таймфреймы
}

# [RECONSTRUCTION 1.6] Stablecoin identification
STABLECOIN_IDENTIFIERS = [
    # USD-привязанные
    'USDT', 'USDC', 'BUSD', 'DAI', 'TUSD', 'USDP', 'USDD', 'GUSD', 'FRAX',
    # EUR-привязанные
    'EURS', 'EURT', 'EUROC',
    # Алгоритмические
    'USTC', 'USDN', 'USDX',
    # Производные
    'CUSD', 'SUSD', 'MUSD', 'XUSD',
    # Идентификаторы в названиях
    'USD', 'STABLE', 'DOLLAR', 'EURO'
]

# [RECONSTRUCTION 1.7] Meme token identification
def is_likely_meme_token(coin_data: Dict) -> bool:
    """Проверка на мем-токен по имени, символу и тегам"""
    meme_indicators = [
        # Базовые
        'MEME', 'DOG', 'SHIB', 'PEPE', 'WOJAK', 'TRUMP', 'FART', 'GOAT',
        'CAT', 'BIRD', 'FISH', 'MOON', 'ELON', 'SAFE', 'BABY', 'APE',
        # Из анализа логов
        'OTTO', 'KEKIUS', 'FWOG', 'RATS', 'PATRIOT', 'HOTDOGE',
        'MIGMIG', 'KAPPY', 'DOGS', 'LAIKA', 'BABYPENGU', 'CHEYENNE',
        'MAGA', 'NEIRO', 'HOTKEY', 'QUILL', 'BOME', 'SQUID', 'LWFI',
        'BTCF', 'GROK', 'LUNARLENS'
    ]
    
    suspicious_tags = [
        'MEMES', 
        'DOGGONE-DOGGEREL',
        'COMMUNITY-TOKENS',
        'FUN-TOKENS',
        'ARTIFICIAL-INTELLIGENCE',
        'GAMING'
    ]
    
    symbol = coin_data['symbol'].upper()
    name = coin_data['name'].upper()
    tags = [tag.upper() for tag in coin_data.get('tags', [])]
    
    return (
        any(indicator in symbol for indicator in meme_indicators) or
        any(indicator in name for indicator in meme_indicators) or
        any(tag in suspicious_tags for tag in tags)
    )

# ============= END BLOCK 1 =========================================

# ============= BLOCK 1.5: BASE MARKET ANALYZER =============

class MarketAnalyzer:
    """
    Базовый класс для анализа рыночных данных.
    Обеспечивает единую категоризацию и базовые метрики для всех типов анализа.
    """
    def __init__(self):
        self.category_thresholds = {
            'market_cap': {
                'major': 10_000_000_000,   # $10B (топ-10 монет)
                'medium': 1_000_000_000,   # $1B
                'small': 0                 # Все, что меньше $1B
            }
        }
        
        self.timeframes = {
            '1h': 'hourly',
            '24h': 'daily',
            '7d': 'weekly'
        }
        
        self.base_weights = {
            'hourly': 0.35,
            'daily': 0.35,
            'weekly': 0.30
        }

    def get_asset_category(self, coin_data: Dict) -> Optional[str]:
        """
        Единый метод определения категории актива
        
        Args:
            coin_data: Данные о монете
            
        Returns:
            Optional[str]: Категория актива или None в случае ошибки
        """
        try:
            if not coin_data or 'quote' not in coin_data:
                logger.error(f"Некорректные входные данные: {coin_data}")
                return None
                
            if is_stablecoin(coin_data):
                return 'stablecoin'
                
            if is_likely_meme_token(coin_data):
                return 'meme_coins'
                
            market_cap = coin_data['quote']['USD'].get('market_cap')
            if market_cap is None:
                logger.error(f"Отсутствует market_cap для {coin_data.get('symbol', 'Unknown')}")
                return None
                
            if market_cap > self.category_thresholds['market_cap']['major']:
                return 'major_coins'
            elif market_cap > self.category_thresholds['market_cap']['medium']:
                return 'medium_coins'
            else:
                return 'small_coins'
                
        except Exception as e:
            logger.error(f"Ошибка определения категории для {coin_data.get('symbol', 'Unknown')}: {e}")
            return None

    def format_metric(self, value: float, percentage: bool = False) -> str:
        """Форматирование метрик"""
        if percentage:
            return f"{value:+.2f}%" if value else "0.00%"
        return MarketDataFormatter.format_number(value)

# ============= END BLOCK 1.5 =========================================

# ============= BLOCK 2: DIRECTORY AND LOGGING SETUP =============

# [RECONSTRUCTION 2.1] Directory creation and validation
for path in PATHS.values():
    os.makedirs(path, exist_ok=True)

def setup_logger():
    """
    Настройка системы логирования с улучшенным форматированием и обработкой
    """
    logger = logging.getLogger('market_data')
    logger.setLevel(logging.INFO)
    
    # Очистка существующих обработчиков
    if logger.handlers:
        logger.handlers.clear()
    
    file_handler = logging.FileHandler(f"{PATHS['LOGS']}fetch_market_data.log")
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    )
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s: %(message)s')
    )
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

def archive_with_access(filename: str, keep_original: bool = True):
    """
    Архивация файла с сохранением доступа и обработкой ошибок
    
    Args:
        filename (str): Имя файла для архивации
        keep_original (bool): Сохранять ли оригинал файла
    """
    try:
        source_path = os.path.join(PATHS['MARKET_CURRENT'], filename)
        if os.path.exists(source_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{filename[:-5]}_{timestamp}.json"
            archive_path = os.path.join(PATHS['MARKET_ARCHIVE'], archive_name)
            
            shutil.copy2(source_path, archive_path)
            
            with open(archive_path, 'rb') as f_in:
                with gzip.open(f"{archive_path}.gz", 'wb') as f_out:
                    f_out.writelines(f_in)
            
            os.remove(archive_path)
            
            if not keep_original:
                os.remove(source_path)
                
            logger.info(f"Файл успешно заархивирован: {archive_name}.gz")
            cleanup_old_archives()
            
    except Exception as e:
        logger.error(f"Ошибка при архивации {filename}: {e}")

def cleanup_old_archives():
    """
    Очистка старых архивных файлов с сохранением только последних версий.
    Удаляет старые архивы, оставляя количество файлов, указанное в CONFIG['KEEP_RECENT_FILES']
    """
    try:
        files = sorted(
            [f for f in os.listdir(PATHS['MARKET_ARCHIVE']) if f.endswith('.gz')],
            key=lambda x: os.path.getmtime(os.path.join(PATHS['MARKET_ARCHIVE'], x)),
            reverse=True
        )
        
        # Оставляем только последние файлы
        for old_file in files[CONFIG['KEEP_RECENT_FILES']:]:
            os.remove(os.path.join(PATHS['MARKET_ARCHIVE'], old_file))
            logger.info(f"Удален старый архив: {old_file}")
            
    except Exception as e:
        logger.error(f"Ошибка при очистке архивов: {e}")

# [RECONSTRUCTION 2.2] Stablecoin validation
def is_stablecoin(coin_data: Dict) -> bool:
    """
    Определение стейблкоина по символу, имени и тегам
    
    Args:
        coin_data (Dict): Данные монеты из API
        
    Returns:
        bool: True если монета определена как стейблкоин
    """
    symbol = coin_data['symbol'].upper()
    name = coin_data['name'].upper()
    
    return (
        any(identifier in symbol for identifier in STABLECOIN_IDENTIFIERS) or
        any(identifier in name for identifier in STABLECOIN_IDENTIFIERS) or
        'stablecoin' in [tag.lower() for tag in coin_data.get('tags', [])]
    )

# [RECONSTRUCTION 2.3] Market data fetching
def fetch_market_data():
    """Получение данных с CoinMarketCap API"""
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }
    params = {
        "start": "1",
        "limit": str(CONFIG['MAX_COINS']),
        "convert": "USD"
    }
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=CONFIG['REQUEST_TIMEOUT']
        )
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса к API: {e}")
        return None

# [RECONSTRUCTION 2.4] Data formatting utilities
class MarketDataFormatter:
    """Утилиты форматирования рыночных данных"""
    
    @staticmethod
    def format_number(number: float, crypto_precision: bool = False) -> str:
        """
        Форматирование числовых значений с учетом размера
        
        Args:
            number (float): Число для форматирования
            crypto_precision (bool): Использовать ли повышенную точность для малых значений
            
        Returns:
            str: Отформатированное значение
        """
        if number is None:
            return "0.00"
        try:
            abs_number = abs(number)
            # Большие числа
            if abs_number >= 1e12: return f"{number/1e12:.2f}T"
            if abs_number >= 1e9: return f"{number/1e9:.2f}B"
            if abs_number >= 1e6: return f"{number/1e6:.2f}M"
            if abs_number >= 1e3: return f"{number/1e3:.1f}K"

            # Малые числа
            if abs_number < 0.01:
                return f"{number:.6f}" if crypto_precision else f"{number:.4f}"
            # Обычные числа
            return f"{number:.2f}"
        except Exception:
            return str(number)
    
    @staticmethod
    def format_percentage(value: float) -> str:
        """
        Форматирование процентных значений
        
        Args:
            value (float): Значение для форматирования
            
        Returns:
            str: Отформатированное процентное значение
        """
        return f"{value:+.2f}%" if value else "0.00%"

# ============= END BLOCK 2 =========================================   
#============= BLOCK 3: LIQUIDITY ANALYSIS =============

class LiquidityAnalyzer(MarketAnalyzer):
    """
    Анализатор ликвидности криптоактивов.
    Обеспечивает комплексный анализ ликвидности с учетом категорий активов
    и корректной обработкой разных типов монет.
    """
    def __init__(self):
        super().__init__()
        
        # Пороговые значения объемов для разных категорий активов
        self.volume_thresholds = {
            'major_coins': {
                'low': 1_000_000_000,     # $1B
                'medium': 5_000_000_000,  # $5B
                'high': 15_000_000_000    # $15B
            },
            'medium_coins': {
                'low': 100_000_000,       # $100M
                'medium': 300_000_000,    # $300M
                'high': 1_000_000_000     # $1B
            },
            'small_coins': {
                'low': 1_000_000,         # $1M
                'medium': 10_000_000,     # $10M
                'high': 100_000_000       # $100M
            },
            'meme_coins': {
                'low': 1_000_000,         # $1M
                'medium': 100_000_000,    # $100M
                'high': 1_000_000_000     # $1B
            }
        }
        
        # VMR пороги для разных категорий
        self.vmr_thresholds = {
            'major_coins': {  
                'low': 1,     # До 1%
                'medium': 3,  # 1-3%
                'high': 5     # >5%
            },
            'medium_coins': {   
                'low': 2,     # До 2%
                'medium': 5,  # 2-5%
                'high': 8     # >8%
            },
            'small_coins': {   
                'low': 3,     # До 3%
                'medium': 8,  # 3-8%
                'high': 12    # >12%
            },
            'meme_coins': {  
                'low': 5,     # До 5%
                'medium': 15, # 5-15%
                'high': 25    # >25%
            }
        }

        # Пороговое значение для определения стабильности объемов
        self.stability_threshold = 50  # 50% изменение объема

    def calculate_vmr(self, volume_24h: float, market_cap: float, category: str) -> Optional[Dict]:
        """
        Расчет VMR (Volume to Market Cap Ratio) с учетом категории актива
        
        Args:
            volume_24h: Объем торгов за 24 часа
            market_cap: Рыночная капитализация
            category: Категория актива
            
        Returns:
            Optional[Dict]: Метрики VMR или None при ошибке
        """
    def calculate_vmr(self, volume_24h: float, market_cap: float, category: str) -> Optional[Dict]:
        try:
            # Безопасная обработка нулевых значений
            if not market_cap or market_cap <= 0:
                return {
                    'value': 0,  # Оставляем для внутренних расчетов
                    'category': 'low',
                    'formatted': "0.00%"
                }
                    
            # Расчет VMR
            vmr = (volume_24h / market_cap * 100)
            
            # Определение категории
            thresholds = self.vmr_thresholds.get(category, self.vmr_thresholds['small_coins'])
            
            vmr_category = 'low'
            if vmr >= thresholds['high']:
                vmr_category = 'high'
            elif vmr >= thresholds['medium']:
                vmr_category = 'medium'
            
            # Оставляем поле value неформатированным для внутренних расчетов
            # но скрываем его от вывода в JSON с помощью _value
            return {
                '_value': vmr,  # Префикс _ указывает, что это внутреннее поле
                'category': vmr_category,
                'formatted': self.format_metric(vmr, percentage=True)
            }
        except Exception as e:
            logger.error(f"Ошибка расчета VMR: volume={volume_24h}, market_cap={market_cap}, error={str(e)}")
            return {
                '_value': 0,
                'category': 'low',
                'formatted': "0.00%"
            }

    def analyze_volume_stability(self, volume_24h: float, volume_change: float, category: str) -> Optional[Dict]:
        """
        Анализ стабильности объемов торгов
        
        Args:
            volume_24h: Объем торгов за 24 часа
            volume_change: Изменение объема в процентах
            category: Категория актива
            
        Returns:
            Optional[Dict]: Метрики стабильности или None при ошибке
        """
        try:
            thresholds = self.volume_thresholds.get(category)
            if not thresholds:
                logger.warning(f"Неизвестная категория: {category}, используем пороги small_coins")
                thresholds = self.volume_thresholds['small_coins']
            
            volume_level = 'low'
            if volume_24h >= thresholds['high']:
                volume_level = 'high'
            elif volume_24h >= thresholds['medium']:
                volume_level = 'medium'
            
            stability = 'stable'
            if abs(volume_change) > self.stability_threshold:
                stability = 'unstable_high' if volume_change > 0 else 'unstable_low'
                
            return {
                'volume_level': volume_level,
                'stability': stability,
                'change_percentage': self.format_metric(volume_change, percentage=True)
            }
        except Exception as e:
            logger.error(f"Ошибка анализа объемов: volume={volume_24h}, change={volume_change}, error={str(e)}")
            return None

    def calculate_liquidity_metrics(self, coin_data: Dict) -> Optional[Dict]:
        """
        Комплексный расчет метрик ликвидности
        
        Args:
            coin_data: Данные о монете
            
        Returns:
            Optional[Dict]: Полные метрики ликвидности или None при ошибке/стейблкоине
        """
        try:
            # Определение категории через базовый класс
            category = self.get_asset_category(coin_data)
            if category is None:
                logger.warning(f"Не удалось определить категорию для {coin_data.get('symbol', 'Unknown')}")
                return None
                
            # Пропуск стейблкоинов
            if category == 'stablecoin':
                return None
                
            # Проверка наличия необходимых данных
            quote = coin_data.get('quote', {}).get('USD', {})
            if not quote:
                logger.error(f"Отсутствуют данные котировок для {coin_data.get('symbol', 'Unknown')}")
                return None
                
            volume_24h = quote.get('volume_24h')
            market_cap = quote.get('market_cap')
            volume_change = quote.get('volume_change_24h')
            num_pairs = coin_data.get('num_market_pairs')
            
            if any(v is None for v in [volume_24h, market_cap, volume_change, num_pairs]):
                logger.error(f"Неполные данные для {coin_data.get('symbol', 'Unknown')}")
                return None
            
            # Расчет метрик
            vmr_data = self.calculate_vmr(volume_24h, market_cap, category)
            volume_stability = self.analyze_volume_stability(volume_24h, volume_change, category)
            
            if not vmr_data or not volume_stability:
                return None
            
            return {
                'category': category,
                'volume_metrics': {
                    'formatted_24h': self.format_metric(volume_24h),
                    'change_24h': volume_stability['change_percentage'],
                    'level': volume_stability['volume_level']
                },
                'vmr': vmr_data,
                'stability': volume_stability['stability'],
                'trading_pairs': num_pairs,
                'avg_volume_per_pair': self.format_metric(
                    volume_24h / num_pairs if num_pairs > 0 else 0
                )
            }
            
        except Exception as e:
            logger.error(
                f"Ошибка расчета ликвидности для {coin_data.get('symbol', 'Unknown')}: "
                f"volume={volume_24h}, market_cap={market_cap}, error={str(e)}"
            )
            return None

# ============= END BLOCK 3 =========================================

# ============= BLOCK 4: VOLATILITY ANALYSIS =============

class VolatilityAnalyzer(MarketAnalyzer):
    """
    Анализатор волатильности криптоактивов.
    Базовый анализ волатильности по основным таймфреймам (1h, 24h, 7d).
    """
    
    # ======== Initialization ========
    def __init__(self):
        super().__init__()

        self.liquidity_analyzer = LiquidityAnalyzer()
        
        self.volatility_thresholds = {
            'major_coins': {   
                'low': 1.5,    # Базовая волатильность
                'medium': 4,   # Средняя волатильность
                'high': 8      # Высокая волатильность
            },
            'medium_coins': {     
                'low': 3,      
                'medium': 8,   
                'high': 15     
            },
            'small_coins': {    
                'low': 5,      
                'medium': 12,  
                'high': 20     
            },
            'meme_coins': {    
                'low': 10,     
                'medium': 20,  
                'high': 35     
            }
        }

    # ======== Core Methods ========
    def _validate_price_data(self, coin_data: Dict) -> bool:
        """Валидация необходимых полей данных"""
        required_fields = ['price', 'volume_24h', 'percent_change_1h', 
                          'percent_change_24h', 'percent_change_7d', 
                          'volume_change_24h', 'market_cap']
        
        try:
            quote = coin_data.get('quote', {}).get('USD', {})
            return all(quote.get(field) is not None for field in required_fields)
        except Exception:
            return False

    def _normalize_changes(self, changes: Dict[str, float]) -> Dict[str, float]:
        """Нормализация изменений цены"""
        return {timeframe: abs(value) for timeframe, value in changes.items()}

    # ======== Main Analysis Methods ========
    def calculate_volatility_metrics(self, coin_data: Dict) -> Optional[Dict]:
        """Расчет метрик волатильности для монеты"""
        try:
            if not self._validate_price_data(coin_data):
                logger.error(f"Неполные данные для {coin_data.get('symbol', 'Unknown')}")
                return None

            category = self.get_asset_category(coin_data)
            if category is None or category == 'stablecoin':
                return None
                
            quote = coin_data['quote']['USD']
            
            changes = {
                '1h': quote['percent_change_1h'],
                '24h': quote['percent_change_24h'],
                '7d': quote['percent_change_7d']
            }
            
            abs_changes = self._normalize_changes(changes)
            
            composite_volatility = self._calculate_composite_index(
                hourly_change=abs_changes['1h'],
                daily_change=abs_changes['24h'],
                weekly_change=abs_changes['7d'],
                volume_change=quote['volume_change_24h']
            )
            
            metrics = {
                'category': category,
                'price_info': {
                    'price': self.format_metric(quote['price']),
                    'volume_24h': self.format_metric(quote['volume_24h']),
                    'volume_change': self.format_metric(quote['volume_change_24h'], percentage=True),
                    'market_cap': self.format_metric(quote['market_cap'])
                },
                'volatility_metrics': {
                    'composite_index': self.format_metric(composite_volatility, percentage=True),
                    'market_state': self._get_volatility_state(composite_volatility, 
                                                             self.volatility_thresholds[category]),
                    'direction': self._determine_trend(changes)
                },
                'movement_analysis': {
                    'amplitude': {k: self.format_metric(v, percentage=True) for k, v in abs_changes.items()},
                    'direction': {k: self.format_metric(v, percentage=True) for k, v in changes.items()}
                }
            }

            # Получаем анализ ликвидности
            liquidity_metrics = self.liquidity_analyzer.calculate_liquidity_metrics(coin_data)
            
            metrics = {
                'category': category,
                'price_info': {
                    'price': self.format_metric(quote['price']),
                    'volume_24h': self.format_metric(quote['volume_24h']),
                    'volume_change': self.format_metric(quote['volume_change_24h'], percentage=True),
                    'market_cap': self.format_metric(quote['market_cap'])
                },
                'volatility_metrics': {
                    'composite_index': self.format_metric(composite_volatility, percentage=True),
                    'market_state': self._get_volatility_state(composite_volatility, 
                                                            self.volatility_thresholds[category]),
                    'direction': self._determine_trend(changes)
                },
                'movement_analysis': {
                    'amplitude': {k: self.format_metric(v, percentage=True) for k, v in abs_changes.items()},
                    'direction': {k: self.format_metric(v, percentage=True) for k, v in changes.items()}
                }
            } 

            # Добавляем метрики ликвидности если они есть
            if liquidity_metrics:
                metrics['liquidity_analysis'] = liquidity_metrics   
            
            self.log_volatility_analysis(coin_data['symbol'], metrics)
            return metrics
                    
        except Exception as e:
            logger.error(f"Ошибка расчета волатильности для {coin_data.get('symbol', 'Unknown')}: {e}")
            return None

    def filter_market_data(self, market_data: List[Dict]) -> List[Dict]:
        """Фильтрация и первичный анализ рыночных данных"""
        try:
            filtered = []
            btc_data = next((coin for coin in market_data if coin['symbol'] == 'BTC'), None)
            
            for coin in market_data:
                quote = coin['quote']['USD']
                
                if (quote['volume_24h'] > CONFIG['VOLUME_THRESHOLD'] and 
                    quote['price'] > CONFIG['MIN_PRICE']):
                    
                    volatility_metrics = self.calculate_volatility_metrics(coin)
                    if volatility_metrics:
                        coin['volatility_metrics'] = volatility_metrics
                        filtered.append(coin)
            
            logger.info(f"Отфильтровано {len(filtered)} монет из {len(market_data)}")
            return filtered
            
        except Exception as e:
            logger.error(f"Ошибка при фильтрации данных: {e}")
            return []
        
    def calculate_market_metrics(self, filtered_data: List[Dict]) -> Optional[Dict]:
        """
        Расчет и анализ общих метрик рынка
        
        Args:
            filtered_data: Список отфильтрованных данных монет
            
        Returns:
            Optional[Dict]: Метрики рынка или None при ошибке
        """
        try:
            raw_metrics = {
                'total_market_cap': 0,
                'total_volume_24h': 0,
                'btc_dominance': 0,
                'total_coins': len(filtered_data),
                'volatility': {
                    'general_market': {
                        'distribution': {
                            'low': 0,
                            'medium': 0,
                            'high': 0
                        }
                    },
                    'meme_sector': {
                        'distribution': {
                            'low': 0,
                            'medium': 0,
                            'high': 0
                        }
                    }
                },
                'liquidity': {
                    'general_market': {
                        'volume_levels': {'low': 0, 'medium': 0, 'high': 0},
                        'stability': {'stable': 0, 'unstable_high': 0, 'unstable_low': 0},
                        'total_vmr': 0,
                        'vmr_count': 0
                    },
                    'meme_sector': {
                        'volume_levels': {'low': 0, 'medium': 0, 'high': 0},
                        'stability': {'stable': 0, 'unstable_high': 0, 'unstable_low': 0},
                        'total_vmr': 0,
                        'vmr_count': 0
                    }
                }
            }

            # Получаем BTC данные для расчета доминации
            btc_data = next((coin for coin in filtered_data if coin['symbol'] == 'BTC'), None)
            
            for coin in filtered_data:
                if not coin.get('volatility_metrics'):
                    continue

                quote = coin['quote']['USD']
                raw_metrics['total_market_cap'] += quote.get('market_cap', 0)
                raw_metrics['total_volume_24h'] += quote.get('volume_24h', 0)
                
                sector = 'meme_sector' if coin['volatility_metrics']['category'] == 'meme_coins' else 'general_market'
                
                # Обработка волатильности
                market_state = coin['volatility_metrics']['volatility_metrics']['market_state']
                raw_metrics['volatility'][sector]['distribution'][market_state] += 1
                
                # Обработка ликвидности
                # Внутри цикла обработки ликвидности
                if 'liquidity_analysis' in coin['volatility_metrics']:
                    liq_data = coin['volatility_metrics']['liquidity_analysis']
                    
                    # Корректный доступ к данным ликвидности
                    volume_level = liq_data['volume_metrics']['level']
                    stability = liq_data['stability']
                    
                    # Используем _value вместо value
                    vmr = liq_data['vmr'].get('_value', 0)
                    if vmr == 0 and 'formatted' in liq_data['vmr']:
                        # Если _value отсутствует, пытаемся получить значение из formatted
                        try:
                            formatted = liq_data['vmr']['formatted'].replace('%', '')
                            vmr = float(formatted)
                        except (ValueError, AttributeError):
                            vmr = 0
                    
                    raw_metrics['liquidity'][sector]['volume_levels'][volume_level] += 1
                    raw_metrics['liquidity'][sector]['stability'][stability] += 1
                    raw_metrics['liquidity'][sector]['total_vmr'] += vmr
                    raw_metrics['liquidity'][sector]['vmr_count'] += 1    

            # Расчет BTC доминации
            if btc_data and raw_metrics['total_market_cap'] > 0:
                btc_market_cap = btc_data['quote']['USD'].get('market_cap', 0)
                raw_metrics['btc_dominance'] = (btc_market_cap / raw_metrics['total_market_cap']) * 100

            # Форматирование результатов
            formatted_metrics = {
                'market_overview': {
                    'total_market_cap': self.format_metric(raw_metrics['total_market_cap']),
                    'total_volume_24h': self.format_metric(raw_metrics['total_volume_24h']),
                    'btc_dominance': f"{raw_metrics['btc_dominance']:.2f}%",
                    'total_coins': raw_metrics['total_coins']
                },
                'sector_analysis': {
                    'general_market': self._format_sector_metrics(
                        raw_metrics, 'general_market', raw_metrics['total_coins']
                    ),
                    'meme_sector': self._format_sector_metrics(
                        raw_metrics, 'meme_sector', raw_metrics['total_coins']
                    )
                }
            }

            return formatted_metrics

        except Exception as e:
            logger.error(f"Ошибка расчета рыночных метрик: {e}")
            return None

    def _format_sector_metrics(self, raw_metrics: Dict, sector: str, total_coins: int) -> Dict:
        try:
            # Подсчет реального количества монет в секторе
            sector_count = sum(raw_metrics['volatility'][sector]['distribution'].values())
            if sector_count == 0:
                return {}
            
            # Проверка наличия метрик ликвидности
            has_liquidity = all(key in raw_metrics['liquidity'][sector] for key in ['volume_levels', 'stability'])
            
            sector_data = {
                'volatility_distribution': {
                    k: f"{(v/sector_count*100):.1f}%"
                    for k, v in raw_metrics['volatility'][sector]['distribution'].items()
                }
            }
            
            # Добавляем метрики ликвидности только если они доступны
            if has_liquidity:
                volume_total = sum(raw_metrics['liquidity'][sector]['volume_levels'].values())
                stability_total = sum(raw_metrics['liquidity'][sector]['stability'].values())
                
                if volume_total > 0 and stability_total > 0:
                    sector_data['liquidity'] = {
                        'volume_distribution': {
                            k: f"{(v/volume_total*100):.1f}%"
                            for k, v in raw_metrics['liquidity'][sector]['volume_levels'].items()
                        },
                        'stability_distribution': {
                            k: f"{(v/stability_total*100):.1f}%"
                            for k, v in raw_metrics['liquidity'][sector]['stability'].items()
                        }
                    }
                    
                    # Добавляем средний VMR если он доступен
                    if raw_metrics['liquidity'][sector].get('vmr_count', 0) > 0:
                        avg_vmr = raw_metrics['liquidity'][sector]['total_vmr'] / raw_metrics['liquidity'][sector]['vmr_count']
                        sector_data['liquidity']['average_vmr'] = f"{avg_vmr:.2f}%"
            
            return sector_data
        except Exception as e:
            logger.error(f"Ошибка форматирования метрик сектора {sector}: {e}")
            return {}

    # ======== Helper Calculation Methods ========
    def _calculate_composite_index(self, hourly_change: float, 
                                 daily_change: float,
                                 weekly_change: float,
                                 volume_change: float) -> float:
        """Расчет композитного индекса волатильности"""
        weights = {
            'hourly': 0.35,
            'daily': 0.35,
            'weekly': 0.30
        }
        
        normalized_volume = min(abs(volume_change), 100) / 100
        
        return (weights['hourly'] * hourly_change +
                weights['daily'] * daily_change +
                weights['weekly'] * weekly_change +
                0.2 * normalized_volume * 100)

    def _determine_trend(self, changes: Dict[str, float]) -> str:
        """Определение базового тренда"""
        try:
            short_term = [changes['1h'], changes['24h']]
            
            if all(change > 0 for change in short_term):
                return 'bullish'
            elif all(change < 0 for change in short_term):
                return 'bearish'
            return 'mixed'
            
        except Exception as e:
            logger.error(f"Ошибка определения тренда: {e}")
            return 'mixed'

    def _get_volatility_state(self, volatility: float, thresholds: Dict) -> str:
        """Определение состояния волатильности"""
        try:
            if volatility < thresholds['low']:
                return 'low'
            elif volatility < thresholds['medium']:
                return 'medium'
            return 'high'
            
        except Exception as e:
            logger.error(f"Ошибка определения состояния волатильности: {e}")
            return 'medium'

    # ======== Data Processing Methods ========
    def is_outlier(self, value: float, is_meme: bool = False) -> bool:
        """Определение аномальных значений волатильности"""
        if value is None:
            return True
            
        try:
            if is_meme:
                return abs(value) > 1000
            return abs(value) > 100
        except Exception:
            return True

    def calculate_safe_average(self, values: List[float], category: str = 'medium_coins') -> float:
        """Расчет среднего значения с исключением выбросов"""
        if not values:
            return 0.0
        
        try:
            is_meme = category == 'meme_coins'
            filtered_values = [v for v in values if not self.is_outlier(v, is_meme)]
            
            if not filtered_values:
                return statistics.median(values)
            
            recent_values = filtered_values[-5:] if len(filtered_values) > 5 else filtered_values
            return sum(recent_values) / len(recent_values)
            
        except Exception as e:
            logger.error(f"Ошибка расчета среднего значения: {e}")
            return 0.0

    # ======== Logging Methods ========
    def log_volatility_analysis(self, coin_symbol: str, metrics: Dict):
        """Логирование результатов анализа волатильности"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'symbol': coin_symbol,
            'metrics': metrics
        }
        #logger.info(f"Volatility analysis completed for {coin_symbol}")
        logger.debug(f"Detailed metrics: {json.dumps(log_entry)}")

def save_data(data: Any, filename: str, is_temporary: bool = False) -> bool:
    """
    Сохранение данных в файл с улучшенной обработкой ошибок
    
    Args:
        data: Данные для сохранения
        filename: Имя файла
        is_temporary: Временный ли файл
    
    Returns:
        bool: Успех операции
    """
    try:
        directory = PATHS['TEMP'] if is_temporary else PATHS['MARKET_CURRENT']
        filepath = os.path.join(directory, filename)
        
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)
            
        logger.info(f"Данные сохранены в {filepath}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении {filename}: {e}")
        return False

def create_status_file(status: str):
    """
    Создание статус-файла для синхронизации с другими скриптами
    
    Args:
        status: Статус выполнения ('success' или 'failed')
    """
    try:
        status_file = os.path.join(PATHS['MARKET_CURRENT'], "market_data_status.json")
        with open(status_file, "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "status": status,
                "next_step": "historical_data"
            }, f, indent=4)
    except Exception as e:
        logger.error(f"Ошибка при создании статус-файла: {e}")

def main():
    """Основная функция для получения и обработки данных о рынке"""
    start_time = datetime.now()
    logger.info("Начало получения рыночных данных")

    try:
        # Создание временной директории
        os.makedirs(PATHS['TEMP'], exist_ok=True)
        
        # Получение и валидация данных
        market_data = fetch_market_data()
        if not market_data:
            create_status_file("failed")
            return

        # Инициализация анализатора и обработка данных
        volatility_analyzer = VolatilityAnalyzer()
        filtered_data = volatility_analyzer.filter_market_data(market_data)
        
        if not filtered_data:
            create_status_file("failed")
            return
        
        # Расчет метрик рынка
        market_metrics = volatility_analyzer.calculate_market_metrics(filtered_data)
        
        # Сохранение метрик
        if market_metrics:
            if not save_data(market_metrics, "market_metrics.json", is_temporary=True):
                logger.error("Ошибка сохранения market_metrics.json")
                create_status_file("failed")
                return
        
        # Сохранение отфильтрованных данных
        if save_data(filtered_data, "filtered_data.json", is_temporary=True):
            # Проверка существования файлов
            market_metrics_path = os.path.join(PATHS['MARKET_CURRENT'], "market_metrics.json")
            filtered_data_path = os.path.join(PATHS['MARKET_CURRENT'], "filtered_data.json")
            
            # Архивация существующих файлов
            if os.path.exists(market_metrics_path):
                archive_with_access("market_metrics.json")
            if os.path.exists(filtered_data_path):
                archive_with_access("filtered_data.json")
            
            # Перемещение файлов
            success = True
            for filename in ["filtered_data.json", "market_metrics.json"]:
                source_path = os.path.join(PATHS['TEMP'], filename)
                dest_path = os.path.join(PATHS['MARKET_CURRENT'], filename)
                
                if os.path.exists(source_path):
                    try:
                        shutil.move(source_path, dest_path)
                    except Exception as e:
                        logger.error(f"Ошибка перемещения {filename}: {e}")
                        success = False
                else:
                    logger.error(f"Файл не найден: {source_path}")
                    success = False
            
            # Статистика обработки
            execution_time = datetime.now() - start_time
            hours, remainder = divmod(execution_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Вывод результатов
            stats = {
                'total_coins': len(market_data),
                'filtered_coins': len(filtered_data),
                'categories': {
                    'stablecoins': sum(1 for coin in market_data if is_stablecoin(coin)),
                    'major_coins': sum(1 for coin in filtered_data if coin['volatility_metrics']['category'] == 'major_coins'),
                    'medium_coins': sum(1 for coin in filtered_data if coin['volatility_metrics']['category'] == 'medium_coins'),
                    'small_coins': sum(1 for coin in filtered_data if coin['volatility_metrics']['category'] == 'small_coins'),
                    'meme_coins': sum(1 for coin in filtered_data if coin['volatility_metrics']['category'] == 'meme_coins')
                }
            }
            
            logger.info(
                f"\n=== Обработка завершена ===\n"
                f"Всего монет: {stats['total_coins']}\n"
                f"Прошли фильтрацию: {stats['filtered_coins']}\n"
                f"Стейблкоинов отфильтровано: {stats['categories']['stablecoins']}\n"
                f"Основных монет: {stats['categories']['major_coins']}\n"
                f"Средних монет: {stats['categories']['medium_coins']}\n"
                f"Мелких монет: {stats['categories']['small_coins']}\n"
                f"Мем-токенов: {stats['categories']['meme_coins']}\n"
                f"Время обработки: {hours:02d}:{minutes:02d}:{seconds:02d}\n"
                f"=========================="
            )
            
            if success:
                create_status_file("success")
            else:
                create_status_file("failed")
        else:
            create_status_file("failed")
            
    except Exception as e:
        logger.error(f"Ошибка в основной функции: {e}")
        create_status_file("failed")

if __name__ == "__main__":
    print("Скрипт запущен")
    if not CMC_API_KEY:
        print("API ключ не найден")
        logger.error("API ключ не найден. Проверьте файл .env")
        exit()
    print("API ключ найден, запускаем main()")
    main()
