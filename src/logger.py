#logging, basically it is used to show the texts + time + who said it + how serious it is...
import logging
import sys

def get_logger(name: str):
    #getting a filename from where the text is coming::
    logger = logging.getLogger(name)

    #setting a level of the seriousness of the text::
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:

        #showing all the logs on the terminal ::
        handler = logging.StreamHandler(sys.stdout)

        #make into a format so,it would look proper when we get a logs on the terminal::
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger