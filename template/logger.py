
class Logger:

    def __init__(self):
        self.log_path = './template/db_log.txt'

    def write_log(self, log_msg):
        with open(self.log_path, 'a+') as log_file:
            log_file.write(log_msg)

    def get_log(self, trans_num):
        logs = []
        with open(self.log_path, 'r') as log_file:
            lines = log_file.readlines()
            for line in lines:
                if int(line.split(',')[0]) == trans_num:
                    logs.append(line)
        return logs
