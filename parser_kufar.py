import json
from dataclasses import astuple

import requests
from bs4 import BeautifulSoup
from models import Notebook
from tqdm import tqdm
from CONNECTOR import DbPostgres


class Parser:
    ALIAS_CHARACTER = {'Производитель': 'manufacturer', 'Диагональ экрана': "diagonal",
                       'Разрешение экрана': 'screen_resolution', 'Операционная система': 'os', 'Процессор': 'processor',
                       'Оперативная память': 'op_mem', 'Тип видеокарты': 'type_video_card', 'Видеокарта': 'video_card',
                       'Тип накопителя': 'type_drive', 'Ёмкость накопителя': 'capacity_drive',
                       'Время автономной работы': 'auto_word_time', 'Состояние': 'state'}

    HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/114.0'}
    DB = DbPostgres()

    def get_links(self, url: str) -> list:
        response = requests.get(url, headers=self.HEADERS)
        soup = BeautifulSoup(response.text, 'lxml')
        cards = soup.find_all('section')
        links = []
        for card in cards:
            a = card.find('a', href=True)['href']
            a = a.split('?')[0]
            links.append(a)
        page = soup.find('script', id='__NEXT_DATA__').text
        pages = json.loads(page)['props']['initialState']['listing']['pagination']
        page = list(filter(lambda el: el['label'] == 'next', pages))[0]['token']
        print(pages)
        return [links, page]

    def get_data(self, urls: list) -> list:
        data = []
        for url in tqdm(urls, desc='PARSING DATA'):
            resp = requests.get(url, self.HEADERS)
            soup = BeautifulSoup(resp.text, 'lxml')
            props_data = {}
            title = soup.find('h1', class_='styles_brief_wrapper__title__Ksuxa').text
            price = soup.find('span', class_='styles_main__eFbJH').text
            price = price.replace(' р.', '').replace(' ', '')
            try:
                price = float(price)
            except Exception as e:
                print('ERROR>>' + price + "<<")
                continue
            description = soup.find('div', itemprop="description").text

            props = soup.find_all('div', {"class": 'styles_parameter_wrapper__L7UfK'})

            for p in props:
                k = p.find('div', class_='styles_parameter_label__i_OkS').text
                v = p.find('div', class_='styles_parameter_value__BkYDy').text
                if k in self.ALIAS_CHARACTER:
                    k = self.ALIAS_CHARACTER[k]
                    props_data[k] = str(v)
            try:
                image = soup.find('img', 'styles_slide__image__YIPad')['src']
            except:
                print('>>>ERROR', url, '<<<')
                image = ''
            props_data['title'] = title
            props_data['price'] = price
            props_data['description'] = description
            props_data['image'] = image
            props_data['url'] = url
            data.append(Notebook(**props_data))
        return data

    def save_data(self, data: list) -> None:
        for i in data:
            self.DB.query_update("""
            INSERT INTO notebook(title, url, price, description, image, manufacturer, diagonal, screen_resolution, os,
            processor, op_mem, type_video_card, video_card, type_drive, capacity_drive, auto_word_time, state) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s) ON CONFLICT DO NOTHING 
            """, astuple(i))

    def run(self):

        url = 'https://www.kufar.by/l/r~minsk/noutbuki?cursor=eyJ0IjoicmVsIiwiYyI6W3sibiI6Imxpc3RfdGltZSIsInYiOjE2OTI4NjkzNzYwMDB9LHsibiI6ImFkX2lkIiwidiI6MjA1NDYwNzg2fV0sImYiOmZhbHNlfQ=='
        flag = True
        while flag:
            links_and_token = self.get_links(url)
            links = links_and_token[0]
            token = links_and_token[1]
            data = self.get_data(links)
            print(data)
            self.save_data(data)
            url = 'https://www.kufar.by/l/r~minsk/noutbuki?cursor=' + token
            if not token:
                flag = False


Parser().run()
