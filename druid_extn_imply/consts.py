
# Query modes

ASYNC_QUERY = 'broker'
TALARIA_INDEXER = 'indexer'
TALARIA_SERVER = 'server'

ASYNC_INITIALIZED = 'INITIALIZED'
ASYNC_RUNNING = 'RUNNING'
ASYNC_COMPLETE = 'COMPLETE'
ASYNC_FAILED = 'FAILED'

TALARIA_KEY = 'talaria'
TALARIA_SERVICE = 'talaria'

# Talaria task filters
ALL_LEADERS = 'ALL'
WAITING_LEADERS = 'WAITING'
RUNNING_LEADRERS = 'RUNNING'
COMPLETED_LEADERS = 'COMPLETED'
FAILED_LEADERS = 'FAILED'

# Async fields
ASYNC_ID_KEY = 'asyncResultId'
ASYNC_STATE_KEY = 'state'
ASYNC_RESULT_FORMAT_KEY = 'resultFormat'
ASYNC_ENGINE_KEY = 'engine'

ASYNC_BROKER_ENGINE = 'Broker'
ASYNC_TALARIA_INDEXER_ENGINE = 'Talaria-Indexer'
ASYNC_TALARIA_SERVER_ENGINE = 'Talaria-Server'