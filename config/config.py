# config/brain/config.py

# ============================================================
# ОСНОВНАЯ КОНФИГУРАЦИЯ BRAIN CORE MODULE
# ============================================================

import os

# Пути к директориям с полной структурой
PATHS = {
    # Основные пути
    'BRAIN_DATA': "data/brain/",
    'BRAIN_LOGS': "logs/brain/",
    
    # Пути для Data Engine
    'DATA_ENGINE': {
        'current': "data/brain/data_engine/current/",
        'archive': "data/brain/data_engine/archive/",
        'temp': "data/brain/data_engine/temp/",
        'cache': "data/brain/data_engine/cache/"
    },
    
    # Пути для логов Data Engine (новая секция)
    'DATA_ENGINE_LOGS': "logs/brain/data_engine/",
    
    # Пути для маркет данных
    'MARKET_CURRENT': "data/market/current/",
    
    # Логи
    'LOGS': "logs/system/",
    'TIMING': "logs/timing/"
}

# Настройки таймфреймов с учетом ограничений API
TIMEFRAMES = {
   # Долгосрочные таймфреймы
    '7d': {
        'api_interval': '1W',           # Формат Binance для недельного интервала
        'history_days': 365,            # 1 год истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'weekly',   # Обновление раз в неделю
        'enabled': True
    },
    '24h': {
        'api_interval': '1d',           # Формат Binance для дневного интервала
        'history_days': 150,            # 5 месяцев истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'daily',    # Ежедневное обновление
        'enabled': True
    },
            
    # Среднесрочные таймфреймы
    '4h': {
        'api_interval': '4h',           # Формат Binance для 4-часового интервала
        'history_days': 120,             # 1 месяц истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'daily',    # Ежедневное обновление
        'enabled': True
    },
    '1h': {
        'api_interval': '1h',           # Формат Binance для часового интервала
        'history_days': 90,             # 1 месяц истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'daily',    # Ежедневное обновление
        'enabled': True
    },
            
    # Краткосрочные таймфреймы
    '30m': {
        'api_interval': '30m',          # Формат Binance для 30-минутного интервала
        'history_days': 30,             # 1 месяц истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'daily',    # Ежедневное обновление
        'enabled': True
    },
    '15m': {
        'api_interval': '15m',          # Формат Binance для 15-минутного интервала
        'history_days': 15,             # 15 дней истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'daily',    # Ежедневное обновление
        'enabled': True
    },
    '5m': {
        'api_interval': '5m',           # Формат Binance для 5-минутного интервала
        'history_days': 5,              # 5 дней истории
        'api_source': 'exchange',       # Используем биржевые данные
        'update_frequency': 'daily',    # Ежедневное обновление
        'enabled': True
                       
    }
}

# Настройки инкрементального обновления
DATA_UPDATE = {
    'incremental': True,            # Использовать инкрементальное обновление
    'overlap_days': 1,              # Перекрытие в 1 день для непрерывности
    'remove_duplicates': True,      # Удалять дубликаты по временным меткам
    'fill_gaps': True,              # Пытаться заполнить пропуски при нерегулярных запусках
    'update_window': 24             # Стандартное окно обновления - 24 часа
}

# Настройка API с поддержкой всех бирж
API = {
    'binance': {
        'enabled': True,
        'base_url': 'https://api.binance.com',
        'endpoints': {
            'klines': '/api/v3/klines',
            'orderbook': '/api/v3/depth',
            'ticker': '/api/v3/ticker/24hr'
        },
        'timeout': 30,
        'retry_count': 2,
        'retry_delay': 2,
        'rate_limit': {
            'requests_per_minute': 1200,
            'safety_margin': 5,
            'delay_between_requests': 0.05
        },
        'default_pair': 'USDT'
    },
    'mexc': {
        'enabled': True,
        'base_url': 'https://api.mexc.com',
        'endpoints': {
            'klines': '/api/v3/klines',
            'orderbook': '/api/v3/depth',
            'ticker': '/api/v3/ticker/24hr'
        },
        'timeout': 30,
        'retry_count': 2,
        'retry_delay': 2,
        'rate_limit': {
            'requests_per_minute': 5000,
            'safety_margin': 5,
            'delay_between_requests': 0.02
        },
        'default_pair': 'USDT',
        'public_api_only': True
    }
}

PROCESSING = {
    'batch_size': 40,           # Размер пакета монет для параллельной обработки
    'max_workers': 24,           # Максимум параллельных процессов
    'dynamic_worker_adjustment': True,  # Динамическая корректировка числа потоков
    
    # Обработка ошибок и повторные попытки
    'retry_attempts': 2,        # Количество повторных попыток при ошибке
    'retry_delay': 2,           # Задержка между попытками (сек)
    
    # Управление данными
    'keep_recent_files': 5,     # Количество сохраняемых файлов
    'data_overlap_days': 1,     # Перекрытие данных для непрерывности
    
    # Технические параметры обработки
    'use_pandas': True,         # Использовать pandas для обработки данных
    'data_validation': True     # Проверка и валидация данных
}

# Настройки хранения данных
STORAGE = {
    'compression': False,           # Не используем сжатие для файлов данных
    'archive': {
        'enabled': True,           # Отключаем автоматическое архивирование
        'frequency': 'monthly',      # Архивация раз в месяц (если включена)
        'keep_versions': 3          # Количество хранимых архивных версий
    },
    'quality_metrics': {
        'enabled': True,            # Включаем метрики качества данных
        'thresholds': {
            'completeness': 0.9,    # Минимальная полнота данных (90%)
            'continuity': 0.9,      # Минимальная непрерывность (90%)
            'freshness': 0.9        # Минимальная свежесть (90%)
        }
    }
}

# Настройки логирования и мониторинга
# Параметры системы логирования и отслеживания работы
LOGGING = {
    # Уровни логирования для компонентов
    'levels': {
        # ИСПРАВЛЕНО: Повышен уровень логирования для Data Engine и API для отладки
        'data_engine': 'INFO',           # Уровень логов DataEngine (DEBUG для отладки)
        'indicator_engine': 'INFO',       # Уровень логов IndicatorEngine
        'strategy_engine': 'INFO',        # Уровень логов StrategyEngine
        'market_data': 'INFO',            # Уровень логов MarketData
        'brain_core': 'INFO',             # Уровень логов BrainCore
        'monitoring': 'INFO'              # Уровень логов мониторинга
    },

        # Ротация файлов логов
    'file_rotation': {
        'max_size': 100 * 1024 * 1024,  # 100MB
        'backup_count': 10
    },

    # Уведомления Telegram
    'telegram': {
        'enabled': True,                  # Уведомления активны
        'high_priority_only': False,      # Отправлять все уведомления
        'signal_notifications': True,     # Уведомления о сигналах
        'retry_count': 3,                # Количество попыток отправки
        'retry_delay': 2.0,              # Задержка между попытками (сек)
        'delay': 1.0                     # Задержка между сообщениями (сек)
    },
    
    # Форматирование логов
    'formatting': {
        'console': '%(asctime)s - %(levelname)s - %(name)s: %(message)s',
        'file': '%(asctime)s - %(levelname)s - %(name)s [%(module)s:%(lineno)d]: %(message)s',
        'date_format': '%Y-%m-%d %H:%M:%S',
        'colored_console': True          # Цветной вывод в консоль
    },
    
    # Компоненты логирования
    'components': {
        'console': True,                 # Вывод в консоль
        'file': True,                    # Запись в файл
        'telegram': True,                # Отправка в Telegram
    }
}

# Настройки мониторинга
# Определяет модули и параметры системы мониторинга
MONITORING = {
    # Базовые модули (запускаются с самого начала)
    'basic': {
        'enabled': True,                  # Базовый мониторинг всегда включен
        'system_resources': True,         # Мониторинг CPU, RAM, диска
        'existing_components': True,      # Проверка существующих компонентов
        'execution_time': True            # Проверка времени выполнения компонентов
    },
    
    # Мониторинг Brain Core
    'brain_core': {
        'enabled': True,                  # Активировано для мониторинга Data Engine
        'data_engine': True,              # Мониторинг DataEngine активен
        'indicator_engine': False,        # Пока отключен (компонент не реализован)
        'strategy_engine': False,         # Пока отключен (компонент не реализован)
        'brain_core_status': False        # Мониторинг общего статуса Brain Core
    },
    
    # Расширенный мониторинг
    'advanced': {
        'enabled': True,                 # Изначально отключен
        'memory_tracking': True,         # Подробный мониторинг памяти
        'process_tracking': False,        # Мониторинг процессов Python
        'disk_usage': True,              # Анализ использования дискового пространства
        'performance_metrics': False      # Метрики производительности компонентов
    },
    
    # Настройки отчетов
    'reporting': {
        'verbose': False,                 # Детализированные отчеты
        'telegram_notifications': True,   # Отправка уведомлений в Telegram
        'high_priority_only': False,      # Отправлять только критические уведомления
        'include_error_details': True,    # Включать детали ошибок в отчеты
        'report_interval': 3600           # Интервал отправки отчетов (сек)
    }
}

# Настройки для машинного обучения (для будущего использования)
# Будет активировано после стабилизации основной системы
ML_CONFIG = {
    'enabled': False,                      # ML отключен на начальном этапе
    'data_collection': True,               # Сбор данных для будущего ML включен
    
    # Признаки для ML
    'features': {
        'technical_indicators': True,      # Технические индикаторы
        'price_action': True,              # Ценовые паттерны
        'volume_profile': True,            # Объемные профили
        'market_sentiment': False          # Рыночные настроения (пока отключено)
    },
    
    # Настройки моделей
    'models': {
        'time_series': {
            'enabled': False,              # Модели временных рядов
            'lookback_periods': 24,        # Количество периодов для анализа
            'prediction_periods': 4,       # Количество периодов для прогноза
            'validation_split': 0.2        # Доля данных для валидации
        },
        'classification': {
            'enabled': False,              # Классификационные модели
            'signal_threshold': 0.75,      # Порог вероятности для сигнала
            'balanced_classes': True       # Балансировка классов
        },
        'ensemble': {
            'enabled': False,              # Ансамблевые методы
            'voting_method': 'weighted',   # Метод голосования (weighted/majority)
            'use_best_only': False         # Использовать только лучшие модели
        }
    },
    
    # Параметры обучения
    'training': {
        'auto_retrain': False,             # Автоматическое переобучение
        'retrain_frequency': 604800,       # Частота переобучения (7 дней в секундах)
        'min_samples': 1000,               # Минимальное количество сэмплов для обучения
        'early_stopping': True,            # Раннее останавливание
        'patience': 10                     # Количество эпох без улучшения
    },
    
    # Настройки интеграции с основной системой
    'integration': {
        'prediction_weight': 0.3,          # Вес ML-предсказаний в общей стратегии
        'fallback_to_traditional': True,   # Использовать традиционные методы при отсутствии ML
        'confidence_threshold': 0.8,       # Порог уверенности для учета ML-предсказаний
        'combine_predictions': True        # Комбинировать ML с традиционными индикаторами
    }
}

# Настройки Telegram уведомлений
# Определяет типы и форматы уведомлений для Telegram
TELEGRAM_NOTIFICATIONS = {
    # Статусы компонентов
    'component_status': {
        'enabled': True,                   # Отправка статусов компонентов
        'success': True,                   # Уведомления об успешном выполнении
        'failure': True,                   # Уведомления об ошибках
        'in_progress': False,              # Уведомления о запуске процессов
        'interval': 3600                   # Интервал отправки статусов (сек)
    },
    
    # Системные уведомления
    'system': {
        'enabled': True,                   # Системные уведомления
        'critical_errors': True,           # Критические ошибки
        'resource_warnings': True,         # Предупреждения о ресурсах
        'startup_shutdown': True,          # Запуск/остановка системы
        'performance_alerts': True         # Предупреждения о производительности
    },
    
    # Форматирование сообщений
    'formatting': {
        'use_markdown': False,             # Использовать Markdown
        'use_html': True,                  # Использовать HTML
        'include_emoji': True,             # Включать эмодзи
        'include_timestamps': True,        # Включать временные метки
        'max_message_length': 4000         # Максимальная длина сообщения
    },
    
    # Настройки отправки
    'sending': {
        'max_retries': 3,                  # Максимальное количество попыток отправки
        'retry_delay': 2,                  # Задержка между попытками (сек)
        'rate_limit_delay': 1,             # Задержка для соблюдения ограничений API (сек)
        'batch_messages': True,            # Объединять несколько сообщений
        'batch_timeout': 5                 # Таймаут для объединения сообщений (сек)
    }
}

# Настройки DATA_ENGINE с обновлением для многих бирж
DATA_ENGINE = {
    'enabled': True,
    'api': {
        'use_exchange_data': True,  # Использовать биржевые данные
        'backfill_missing': True    # Заполнять пропуски
    },
    'exchange': {
        'preferred': ['mexc', 'binance'],  # Приоритет бирж как список
        'update_frequency': 'daily',
        'orderbook_depth': 20,
        'max_days_history': {
            '5m': 5,
            '15m': 15,
            '30m': 30
        },
        'rate_limiting': {
            'enabled': True,
            'safety_margin': 5,
            'delay_between_requests': 0.02
        }
    },
    'processing': {
        'merge_incremental': True,
        'deduplicate': True,
        'validate_data': True,
        'batch_size': 40,           # Добавить этот параметр (если он используется в этой секции)
        'parallel_requests': True,    # Добавить опцию для параллельных запросов
        'max_workers': 24              # Добавлено

    },
    'archiving': {
        'enabled': True,
        'frequency': 'monthly',
        'command': ['tar', '-czf', '{archive_path}', '-C', '{current_dir}', '.']
    },
    'telegram': {
        'enabled': True,  # Включить уведомления Telegram для Data Engine
        'success_notifications': True,  # Уведомления об успешном выполнении
        'error_notifications': True     # Уведомления об ошибках
    }
}

# ===========================================================================
# ДОПОЛНЕНИЯ ДЛЯ INDICATOR ENGINE
# ===========================================================================

# Пути для индикаторов и сигналов (добавить в PATHS)
PATHS.update({
    # Пути для Indicator Engine
    'INDICATORS': {
        'current': "data/brain/indicators/current/",
        'archive': "data/brain/indicators/archive/",
        'temp': "data/brain/indicators/temp/"
    },
    
    # Пути для сигналов
    'SIGNALS': {
        'current': "data/brain/signals/current/",
        'archive': "data/brain/signals/archive/",
        'temp': "data/brain/signals/temp/"
    },
    
    # Путь для ордербуков
    'ORDERBOOKS': "data/brain/data_engine/current/orderbooks/",
    
    # Пути для логов Indicator Engine
    'INDICATORS_LOGS': "logs/brain/indicators/"
})

# Настройки индикаторов
INDICATORS = {
    # ЭТАП 1: Основные индикаторы (будут реализованы в первую очередь)
    'phase1': {
        # Активно с начала разработки
        'enabled': True,                     # Этот этап активен сразу
        
        # Трендовые индикаторы
        'trend': {
            'MACD': {                        # Moving Average Convergence Divergence
                'enabled': True,             # Индикатор активен
                'fast_period': 12,           # Период быстрой EMA
                'slow_period': 26,           # Период медленной EMA
                'signal_period': 9,          # Период сигнальной линии
                'weight': 0.4                # Вес индикатора
            },
            'EMA': {                         # Exponential Moving Average
                'enabled': True,             # Индикатор активен
                'periods': [9, 21, 50],      # Периоды EMA
                'weight': 0.3                # Вес индикатора
            }
        },
        
        # Индикаторы моментума
        'momentum': {
            'RSI': {                         # Relative Strength Index
                'enabled': True,             # Индикатор активен
                'period': 14,                # Период расчета
                'overbought': 70,            # Уровень перекупленности
                'oversold': 30,              # Уровень перепроданности
                'weight': 0.35               # Вес индикатора
            }
        },
        
        # Индикаторы волатильности
        'volatility': {
            'ATR': {                         # Average True Range
                'enabled': True,             # Индикатор активен
                'period': 14,                # Период расчета
                'weight': 0.5                # Вес индикатора
            }
        }
    },
    
    # ЭТАП 2: Расширенные индикаторы (будут добавлены после базовых)
    'phase2': {
        # Отключено до реализации базовых индикаторов
        'enabled': False,                    # Этот этап пока отключен
        
        # Трендовые индикаторы (расширенные)
        'trend': {
            'ADX': {                         # Average Directional Index
                'enabled': False,            # Индикатор пока отключен
                'period': 14,                # Период расчета
                'threshold': 25,             # Порог силы тренда
                'strong_trend': 30,          # Уровень сильного тренда
                'weight': 0.3                # Вес индикатора
            }
        },
        
        # Индикаторы моментума (расширенные)
        'momentum': {
            'Stochastic': {                  # Стохастический осциллятор
                'enabled': False,            # Индикатор пока отключен
                'k_period': 14,              # Период %K
                'd_period': 3,               # Период %D
                'smooth_k': 3,               # Сглаживание %K
                'weight': 0.3                # Вес индикатора
            }
        },
        
        # Объемные индикаторы
        'volume': {
            'OBV': {                         # On-Balance Volume
                'enabled': False,            # Индикатор пока отключен
                'ema_period': 20,            # Период EMA для OBV
                'weight': 0.2                # Вес индикатора
            }
        },
        
        # Индикаторы волатильности (расширенные)
        'volatility': {
            'Bollinger': {                   # Bollinger Bands
                'enabled': False,            # Индикатор пока отключен
                'period': 20,                # Период расчета
                'std_dev': 2,                # Стандартное отклонение
                'weight': 0.5                # Вес индикатора
            }
        }
    },
    
    # ЭТАП 3: Продвинутые индикаторы (добавляются в последнюю очередь)
    'phase3': {
        # Отключено до реализации первых двух этапов
        'enabled': False,                    # Этот этап пока отключен
        
        # Продвинутые объемные индикаторы
        'volume': {
            'Delta': {                       # Delta Volume
                'enabled': False,            # Индикатор пока отключен
                'weight': 0.4,               # Вес индикатора
                'smoothing_period': 5,       # Период сглаживания
                'bull_bias': 0.7,            # Смещение для бычьих свечей
                'bear_bias': 0.7,            # Смещение для медвежьих свечей
                'convergence_threshold': 0.7, # Порог конвергенции
                'divergence_threshold': 0.3   # Порог дивергенции
            },
            'VSA': {                         # Volume Spread Analysis
                'enabled': False,            # Индикатор пока отключен
                'weight': 0.4,               # Вес индикатора
                'volume_threshold': 1.5,     # Порог объема
                'lookback_period': 20,       # Период для анализа
                'climax_detection': True     # Обнаружение кульминаций
            }
        }
    }
}

# Настройки для детекторов паттернов
PATTERNS = {
    'enabled': True,                  # Включить обнаружение паттернов
    
    'price': {                        # Ценовые паттерны
        'enabled': True,
        'min_pattern_length': 5,      # Минимальная длина паттерна
        'max_pattern_length': 30,     # Максимальная длина паттерна
        'deviation_threshold': 0.03,  # Порог отклонения для идентификации паттернов
        'head_shoulders_threshold': 0.02  # Порог для идентификации головы и плеч
    },
    
    'volume': {                       # Объемные паттерны
        'enabled': True,
        'volume_threshold': 2.0,      # Порог для высокого объема
        'lookback_period': 20         # Период для анализа
    },
    
    'volatility': {                   # Паттерны волатильности
        'enabled': True,
        'lookback_period': 20,        # Период для анализа
        'volatility_threshold': 2.0   # Порог для высокой волатильности
    },
    
    'combined': {                     # Комбинированные паттерны
        'enabled': True,
        'confidence_bonus': 0.1       # Бонус уверенности для комбинаций
    }
}

# Настройки для детекторов дивергенций
DIVERGENCES = {
    'enabled': True,                  # Включить обнаружение дивергенций
    
    'RSI': {                          # Дивергенции RSI
        'enabled': True,
        'window': 5,                  # Окно для поиска экстремумов
        'min_divergence_points': 2,   # Минимальное количество точек для дивергенции
        'min_threshold': 0.03         # Минимальный порог для дивергенции (3%)
    },
    
    'MACD': {                         # Дивергенции MACD
        'enabled': True,
        'window': 5,                  # Окно для поиска экстремумов
        'min_divergence_points': 2,   # Минимальное количество точек для дивергенции
        'min_threshold': 0.03         # Минимальный порог для дивергенции (3%)
    },
    
    'Stochastic': {                   # Дивергенции Stochastic
        'enabled': False,             # Пока отключено
        'window': 5,                  # Окно для поиска экстремумов
        'min_divergence_points': 2,   # Минимальное количество точек для дивергенции
        'min_threshold': 0.03         # Минимальный порог для дивергенции (3%)
    },
    
    'OBV': {                          # Дивергенции OBV
        'enabled': False,             # Пока отключено
        'window': 5,                  # Окно для поиска экстремумов
        'min_divergence_points': 2,   # Минимальное количество точек для дивергенции
        'min_threshold': 0.03         # Минимальный порог для дивергенции (3%)
    }
}

# Настройки для анализаторов ордербуков
ORDERBOOK_ANALYSIS = {
    'enabled': True,                  # Включить анализ ордербуков
    
    'liquidity': {                    # Анализатор ликвидности
        'enabled': True,
        'volume_threshold': 2.0,      # Порог для кластеров ликвидности
        'levels_to_detect': 5         # Количество уровней ликвидности для обнаружения
    },
    
    'imbalance': {                    # Анализатор имбаланса
        'enabled': True,
        'imbalance_threshold': 2.0,   # Порог для имбаланса
        'price_level_groups': 5       # Количество групп цен для анализа
    },
    
    'support_resistance': {           # Анализатор уровней поддержки и сопротивления
        'enabled': True,
        'volume_threshold': 2.0,      # Порог для уровней
        'max_levels': 5,              # Максимальное количество уровней для определения
        'price_grouping': 0.005       # Группировка цен (0.5%)
    }
}

# Настройки компонента INDICATOR_ENGINE
INDICATOR_ENGINE = {
    'enabled': True,                  # Включить индикаторный движок
    
    # Весовая система таймфреймов
    'timeframe_weights': {
        '7d': 0.1,                    # 10%
        '24h': 0.1,                   # 10%
        '4h': 0.25,                   # 25%
        '1h': 0.25,                   # 25%
        '30m': 0.1,                   # 10%
        '15m': 0.1,                   # 10%
        '5m': 0.1                     # 10%
    },
    
    # Настройки обработки
    'processing': {
        'batch_size': 40,             # Размер пакета монет для параллельной обработки
        'max_workers': 24,            # Максимальное количество параллельных процессов
        'dynamic_worker_adjustment': True,  # Динамическая корректировка числа потоков
        'retry_attempts': 2,          # Количество повторных попыток при ошибке
        'retry_delay': 2              # Задержка между попытками (сек)
    },
    
    # Настройки фильтрации монет
    'filtering': {
        'enabled': True,              # Включить фильтрацию монет
        'min_signals': 3,             # Минимальное количество сигналов для монеты
        'min_confidence': 0.6,        # Минимальная уверенность сигнала
        'max_symbols_percent': 15     # Максимальный процент монет после фильтрации
    },
    
    # Настройки для режима тестирования
    'testing': {
        'enabled': False,             # Режим тестирования отключен по умолчанию
        'symbols': ["BTC", "ETH", "XRP", "SOL", "BNB"],  # Монеты для тестирования
        'full_test': False            # Полное тестирование (все таймфреймы)
    },
    
    # Настройки для интеграции с ордербуками
    'orderbook': {
        'enabled': True,              # Включить интеграцию с ордербуками
        'analyze_all_symbols': False, # Анализировать ордербуки для всех символов
        'top_symbols_only': True,     # Анализировать ордербуки только для топ символов
        'max_symbols': 20             # Максимальное количество символов для анализа ордербуков
    },
    
    # Интеграция с Telegram
    'telegram': {
        'enabled': True,              # Включить уведомления в Telegram
        'send_signals': True,         # Отправлять уведомления о сигналах
        'send_top_symbols': True,     # Отправлять топ символы
        'top_symbols_count': 10       # Количество топ символов для отправки
    }
}

# Обновление настроек мониторинга для индикаторного движка
MONITORING['brain_core']['indicator_engine'] = True
