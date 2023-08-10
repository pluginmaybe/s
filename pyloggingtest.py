import sys
import logging
import cmc_csv_convertor


def main():
    log_path = "./logs/"
    file_name = "filelogtest.log"

    logging.basicConfig(
        filename=log_path + file_name, 
        level=logging.INFO,
        format='%(levelname)s:%(asctime)s:%(message)s')
    
    logging.info("App started")
    print(sys.argv)
    args = sys.argv

    if len(args) <= 1:
        logging.warning("Boo!!")
    else:
        logging.info(args[1])

    result = cmc_csv_convertor.csv_creator()

    logging.info(result)

    



if __name__ == '__main__':
    main()