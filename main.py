# %%
import builtins as exceptions
import logging

#%%
logging.basicConfig(level=logging.INFO, filename='logs/app.log', filemode='a', format='%(asctime)s : %(module)s - %(funcName)s - %(lineno)d : %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.info('Starting pipeline')
#%%
try:
    
    logging.info('Starting load to bronze')
    exec(open('1_load.py').read())
    logging.info('Finished load to bronze')

    logging.info('Starting clean bronze')
    exec(open('2_clean.py').read())
    logging.info('Finished clean bronze')

    logging.info('Starting validation on silver data')
    exec(open('3_validate.py').read())
    logging.info('Finished validation on silver data')

    logging.info('Finishing pipeline')

except exceptions.Exception as e:
    logging.error(f'Fatal excpetion: {e}')
