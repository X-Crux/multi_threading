import glob
import os
import threading
import time


class CSVParse(threading.Thread):

    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.stat = []
        self.zero_stat = []

    def run(self):
        for filename in glob.glob(os.path.join(self.path, '*.csv')):
            normal_filename = filename.split('/')[-1].split('.')[0]

            with open(os.path.join(os.getcwd(), filename), 'r') as csv_file:
                quantity = []

                for line in csv_file:
                    list_line = line[:-1].split(',')

                    if list_line[2] != 'PRICE':
                        quantity.append(float(list_line[2]))

                self._data_processing(normal_filename, quantity)

        self.stat = sorted(self.stat, reverse=True)
        self.zero_stat = sorted(self.zero_stat)

    def _data_processing(self, normal_filename, quantity):
        average_price = (max(quantity) + min(quantity)) / 2
        volatility = ((max(quantity) - min(quantity)) / average_price) * 100
        round_volatility = float("{:.2f}".format(volatility))
        if round_volatility == 0:
            self.zero_stat.append(normal_filename)
        else:
            self.stat.append((round_volatility, normal_filename))


def time_track(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()

        result = func(*args, **kwargs)

        ended_at = time.time()
        elapsed = round(ended_at - started_at, 4)
        print(f'Функция работала {elapsed} секунд(ы)')
        return result

    return surrogate


@time_track
def main():
    directory = '/home/lucas/PycharmProjects/multi_threading/trades/'  # указать абсолютный путь до файлов .csv
    chek_valid = CSVParse(directory)
    chek_valid.start()
    chek_valid.join()

    print('Максимальная волатильность:')
    for i in range(0, 3):
        print(f'\t{chek_valid.stat[i][1]} - {chek_valid.stat[i][0]} %')
    print('Минимальная волатильность:')
    for i in range(0, 3):
        print(f'\t{sorted(chek_valid.stat)[i][1]} - {sorted(chek_valid.stat)[i][0]} %')
    print('Нулевая волатильность:')
    print(f'\t{(", ").join(chek_valid.zero_stat)}')


if __name__ == '__main__':
    main()
