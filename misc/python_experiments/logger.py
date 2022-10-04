class Logger():
    def __init__(self):
        self.f = open('metrics.txt', 'a+')
        self.model = 1

    def log(self, text):
        self.f.write(text)

    def log_train(self, col, time, type=""):
        if self.f.closed:
            self.f = open('metrics.txt', 'a+')
        self.f.write(str(self.model)+";"+str(col)+";"+type+";"+str(time)+"\n")

    def log_test(self, column, metric, performance):
        if self.f.closed:
            self.f = open('results.txt', 'a+')

        self.f.write(str(self.model)+";"+str(column)+";"+str(metric)+";"+str(performance)+"\n")

    def log_sklearn(self, time):
        if self.f.closed:
            self.f = open('metrics.txt', 'a+')
        self.f.write(str(self.model)+";"+str(0)+";"+str(0)+";"+str(time)+"\n")
        self.f.close()

    def next_model(self):
        self.model += 1
        self.f.close()

    def close(self):
        self.f.close()