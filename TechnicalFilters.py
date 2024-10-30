import sys
import basic
import os
from dotenv import dotenv_values

if __name__ == '__main__':
    config = dotenv_values(".env")
    strategy = basic.TechnicalFilters(
        config_file_name = config['CONFIG_FILE_NAME'],  # =sys.argv[1]
        broker_type = config['BROKER_TYPE'],  # =sys.argv[2]
        api_key = config['API_KEY'],  # =sys.argv[3]
        token = config['TOKEN'],  # =sys.argv[4]
        kafka_bootstrap_servers = config['KAFKA_BOOTSTRAP_SERVERS'],  # =sys.argv[5]
        management_topic = config['MANAGEMENT_TOPIC'],  # =sys.argv[6]
        broadcast_topic = config['BROADCAST_TOPIC'],  # =sys.argv[7]
        # paper=sys.argv[8],
        # sub_broker_config=sys.argv[9],
        # datasource_type=sys.argv[8],
        # datasource_user=sys.argv[9],
        # datasource_password=sys.argv[10],
        # paper_flag=sys.argv[11],
        # sub_broker_config=sys.argv[12]
    )
    # try:
    #     strategy.influx_strategy = sys.argv[13]
    # except (KeyError, IndexError):
    #     strategy.influx_strategy = 'basic'
    # try:
    #     strategy.strategy_version = sys.argv[14]
    # except (KeyError, IndexError):
    #     strategy.strategy_version = '1'
    strategy.start()