#!/usr/bin/env python3
import json
import re
import requests
import msgpack
import datetime
import csv

from pprint import pprint 

LISTADO_AVISOS = 'http://www.zonaprop.com.ar/listado_avisos.ajax'
ITEM_AVISO = 'http://www.zonaprop.com.ar/listado_buscarAvisoMapa.ajax'
SPACE_REMOVER = re.compile(r'\s+')
geofence = [ -34.600570, -34.592798, -58.479538, -58.414478 ] # tati
geofence = [ -34.605781, -34.564231, -58.462801, -58.416281 ] # david

keys = [
    'geoloc_1_coordinate',
    'geoloc_0_coordinate',
    'tituloOriginal',
    'fotos',
    'descripcionTipoPropiedadTipoOperacion',
    'tipoDePropiedad',
    'tipoDeOperacion',
    'habitaciones',
    'ambientes',
    'banos',
    'ubicacion',
    'descripcion',
    'm2cubiertos',
    'm2totales',
    'direccion',
    'precioformateado',
    'url',
    ]

csv_keys = [
    'url',
    'tituloOriginal',
    'tipoDePropiedad',
    'habitaciones',
    'ambientes',
    'banos',
    'ubicacion',
    'm2cubiertos',
    'm2totales',
    'direccion',
    'precioformateado'
]

stupid_keys = ['habitaciones', 'banos', 'ambientes']
params = {
    'lat1': geofence[0],
    'lat2': geofence[1],
    'lng1': geofence[2],
    'lng2': geofence[3],
    'volverABuscarAca': True,
    'nivelZoom': 13,
    'parametrosFiltro': '/ph-alquiler-capital-federal-mas-59-m2-menos-20002-pesos-map.html'
}


def fix_item(item):
    ret = {}
    for key, value in item.items():
        if key not in keys:
            continue
        elif key in stupid_keys:
            value = value['valor']
        elif key == 'fotos':
            value = fix_photos(value)
        elif key == 'descripcion':
            value = SPACE_REMOVER.sub(' ', value)
        elif key == 'precioformateado':
            try:
                value = int(''.join(re.findall(r'(\d+)', value)))
            except Exception as e:
                print(value)
                print(e)
                value = 0
        elif key == 'url':
            value = 'http://www.zonaprop.com.ar' + value

        ret[key] = value
    return ret


def fix_photos(ls):
    ret = []
    for item in ls:
        ret.append(item['grande']['url'])
    return ret


def is_useful(item):
    words = ['reservado', 'alquilado']
    if not contained(item, geofence):
        return False
    for word in words:
        if word in item['descripcion'].lower():
            return False
        if word in item['tituloOriginal'].lower():
            return False


    return True


def contained(item, geofence):
    """ geofence must be:
    lat_min, lat_max, lat_min, lat_max
    """
    if (item['geoloc_0_coordinate'] < geofence[0] or
        item['geoloc_0_coordinate'] > geofence[1] or
        item['geoloc_1_coordinate'] < geofence[2] or
        item['geoloc_1_coordinate'] > geofence[3]):
       return False
    return True


def get_item(id_aviso):
    r = requests.get(ITEM_AVISO, params={'idAviso': id_aviso})
    aviso = json.loads(r.text)
    return fix_item(aviso['contenido']['avisos'][0])


def get_list(params):
    mocking = True
    mocking = False
    if mocking:
        data = open('zp', 'r').read()
    else:
        r = requests.get(LISTADO_AVISOS, params=params)
        data = r.text
        open('zp', 'w').write(data)
    data = json.loads(data)
    return data['contenido']['avisosMap']


def save_avisos(ls):
    data = {}
    print("Total items:", len(ls))
    start = datetime.datetime.now()
    for item in ls:
        item_id = item['idAviso']
        item = get_item(item_id)
        item['useful'] = is_useful(item)
        data[item_id] = item
        print(item_id)
    end = datetime.datetime.now()
    td = end - start
    print("Total time:", td.seconds)
    open('alldata', 'wb').write(msgpack.packb(data))
    return data

ls = get_list(params)
mmocking = True
mmocking = False
if mmocking:
    avisos = msgpack.unpackb(open('alldata', 'rb').read(), encoding='utf-8')
else:
    avisos = save_avisos(ls)

useful = list(filter(lambda x: x['useful'], avisos.values()))

with open('phs.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(csv_keys)
    for item in useful:
        values = []
        for key in csv_keys:
            value = ''
            if key in item:
                value = item[key]
            values.append(value)
        writer.writerow(values)
