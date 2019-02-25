#!/usr/bin/python
# -*- coding: utf-8 -*-
import random
import json
from collections import OrderedDict
import argparse

class Generator(object):

    eng_text = ('The Lalamove way has revolutionised '
                'old van hiring call centres'
                ' to being so streamlined,'
                ' \r\n customers and drivers match with each '
                'other within 12 seconds. '
                'Local deliveries are fulfilled at a breakneck '
                '55 minutes, door-to-door. '
                'Providing reliable and quick deliveries for '
                'customers, Lalamove also optimises drivers '
                'fleet/route to maximise their earning potential. '
                'Just Lalamove it!\\\\')

    chin_text = (u'Lalamove\n 送嘢拿拿聲\t 一小時內乜都送到手 '
                 u'\n\nLalamove 為你提供最快和最可靠的物流運送 \n低運輸成本助你提升業務利潤'
                 u'\t\n高效率的貨運服務正好滿足你的業務需要 \n與公司車隊相比節省高達70%！'
                 u' \n即時追蹤訂單狀態 \n使用手機應用程式或網上平台可知道物流實時狀態和司機位置， '
                 u'確保訂單的準時及準確性 \n簡單方便的運送體驗 \r\n網上即時報價，'
                 u'使用電子錢包更無需現金支付車費 '
                 u'隨時隨地查看訂單資料透過手機應用程式或網上平台能清晰查看完整的訂單記錄 令你有更全面的物流管理！'
                 u'可靠及專業的司機團隊\nLalamove 的配對率高達99.5%！'
                 u'絕對是你的物流好拍檔！\n\\')
    keys = tuple(set(eng_text.split(' ') + \
        chin_text.split(' ')))

    def __init__(self, drivers, start_time, records):
        self.data = OrderedDict()
        self.drivers = drivers
        self.start_time = start_time
        self.records = records

    def generate_driver_id(self, junk=False):
        return {'driver_id': random.randint(12471928, 12471928 + self.drivers - 1) + junk * 10000000}

    def generate_timestamp(self, junk=False):
        timestamp = self.start_time

        if junk:
            timestamp += random.randint(-10000, 10000)

        data = {'timestamp': timestamp}

        return data

    def generate_text(self):
        key = random.choice(self.keys)[:]

        if random.random() <= 0.4:
            txt = self.eng_text
        else:
            txt = self.chin_text

        # Generate function
        prob = random.random()
        generate_function = (self.generate_driver_id
                             if random.random() < 0.5 else self.generate_timestamp)

        prob = random.random()

        if prob <= 0.35:
            insert_loc = random.randint(0, len(txt)-1)
            txt = txt[:insert_loc] + \
                json.dumps(generate_function(junk=True)) + txt[insert_loc:]
        elif prob <= 0.55:
            txt = [generate_function(junk=True), txt]
            random.shuffle(txt)
        elif prob <= 0.75:
            txt = generate_function(junk=True)
        elif prob <= 0.8:
            txt = None
        else:
            txt = txt[:random.randint(25, len(txt))]

        return {key: txt}

    def generate_on_duty(self):
        on_duty = 1

        # 20% chance drivers are off duty
        if random.random() < 0.2:
            on_duty = 0
        return {'on_duty': on_duty}

    def get_json(self):
        generate_list = (
            [self.generate_driver_id, self.generate_timestamp, self.generate_on_duty]
            + [self.generate_text] * random.randint(3, 10)
        )

        random.shuffle(generate_list)
        for func in generate_list:
            self.data.update(func())
        return json.dumps(self.data)

    def start(self):
        count = 0
        while count < self.records:
            print self.get_json()
            self.data = OrderedDict()

            # Update total records
            count += 1

            prob = random.random()
            if prob <= 0.1:
                self.start_time += random.randint(-2, -1)
            elif prob <= 0.6:
                self.start_time += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Data generator')
    parser.add_argument('--seed',
                        dest='seed',
                        type=int,
                        help='seed for data generator')
    parser.add_argument('--duration',
                        dest='duration',
                        type=int,
                        help='Duration of data to generate, in seconds')
    parser.add_argument('--records',
                        dest='records',
                        type=int,
                        help='Number of records to generate')
    parser.add_argument('--start_time',
                        dest='start_time',
                        type=int,
                        help='Starting epoch time for the generated records')
    parser.add_argument('--drivers',
                        dest='drivers',
                        type=int,
                        help='Total number of drivers')

    parser.set_defaults(seed=100,
                        records=100000,
                        start_time=1514764800,
                        drivers=500
                        )

    args = parser.parse_args()

    random.seed(args.seed)
    generator = Generator(
        drivers=args.drivers, start_time=args.start_time, records=args.records)
    generator.start()
