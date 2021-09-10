# from celery import Celery
#
# from IoCEngine.config.pilot import BROKER_URL
#
# app = Celery('IoCEngine',
#              broker=BROKER_URL,
#              backend=BROKER_URL,
#              include=['the_process']
#              ,
#              task_serializer=['pickle'],
#              result_serializer='json',
#              accept_content=['pickle']
#              )
# # app.config_from_object('IoCEngine.config.pilot')
#
# app.config_from_object('IoCEngine.config.pilot')
