# test_config.py

def test_parquez():
    import subprocess
    import sys
    errno = subprocess.call([sys.executable, '../core/parquez.py', '--view-name', 'parquezView','--partition-by', '1h'
                                ,'--partition-interval', '1h'
                          ,'--real-time-window', '3h'
                          ,'--historical-retention', '21h'
                          ,'--real-time-table-name', 'booking_service_kv' ,'--config', '../config/parquez.ini'])




