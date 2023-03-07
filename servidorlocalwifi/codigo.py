from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import psycopg2
import os
import pytz
from datetime import datetime
import requests

URL_API = os.environ.get("URL_API")
NUMERO_BOT = os.environ.get("NUMERO_BOT")
APIKEY_BOT = os.environ.get("APIKEY_BOT")
hostName = '0.0.0.0'
serverPort = 43157
conn = None
cursor = None
dias_semana = ("Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo")
ultimahora = datetime.strptime('23:59:59', '%H:%M:%S').time()
primerahora = datetime.strptime('00:00:00', '%H:%M:%S').time()
total=0
CONTRATO=os.environ.get("CONTRATO")

razon1=os.environ.get("RAZON_TELEFONO1")
razon2=os.environ.get("RAZON_TELEFONO2")
razon3=os.environ.get("RAZON_TELEFONO3")
razon4=os.environ.get("RAZON_TELEFONO4")
razon5=os.environ.get("RAZON_TELEFONO5")
razon6=os.environ.get("RAZON_TELEFONO6")
razon7=os.environ.get("RAZON_TELEFONO7")
razon8=os.environ.get("RAZON_TELEFONO8")
razon9=os.environ.get("RAZON_TELEFONO9")
razon10=os.environ.get("RAZON_TELEFONO10")
razon11=os.environ.get("RAZON_TELEFONO11")
razon12=os.environ.get("RAZON_TELEFONO12")
razon13=os.environ.get("RAZON_TELEFONO13")
razon14=os.environ.get("RAZON_TELEFONO14")
razon15=os.environ.get("RAZON_TELEFONO15")
razon16=os.environ.get("RAZON_TELEFONO16")
razon17=os.environ.get("RAZON_TELEFONO17")
razon18=os.environ.get("RAZON_TELEFONO18")
razon19=os.environ.get("RAZON_TELEFONO19")
razon20=os.environ.get("RAZON_TELEFONO20")


razonhuella1=os.environ.get("RAZON_CAPTAHUELLA1")
razonhuella2=os.environ.get("RAZON_CAPTAHUELLA2")
razonhuella3=os.environ.get("RAZON_CAPTAHUELLA3")
razonhuella4=os.environ.get("RAZON_CAPTAHUELLA4")
razonhuella5=os.environ.get("RAZON_CAPTAHUELLA5")
razonhuella6=os.environ.get("RAZON_CAPTAHUELLA6")
razonhuella7=os.environ.get("RAZON_CAPTAHUELLA7")
razonhuella8=os.environ.get("RAZON_CAPTAHUELLA8")
razonhuella9=os.environ.get("RAZON_CAPTAHUELLA9")
razonhuella10=os.environ.get("RAZON_CAPTAHUELLA10")
razonhuella11=os.environ.get("RAZON_CAPTAHUELLA11")
razonhuella12=os.environ.get("RAZON_CAPTAHUELLA12")
razonhuella13=os.environ.get("RAZON_CAPTAHUELLA13")
razonhuella14=os.environ.get("RAZON_CAPTAHUELLA14")
razonhuella15=os.environ.get("RAZON_CAPTAHUELLA15")
razonhuella16=os.environ.get("RAZON_CAPTAHUELLA16")
razonhuella17=os.environ.get("RAZON_CAPTAHUELLA17")
razonhuella18=os.environ.get("RAZON_CAPTAHUELLA18")
razonhuella19=os.environ.get("RAZON_CAPTAHUELLA19")
razonhuella20=os.environ.get("RAZON_CAPTAHUELLA20")

razonrfid1=os.environ.get("RAZON_RFID1")
razonrfid2=os.environ.get("RAZON_RFID2")
razonrfid3=os.environ.get("RAZON_RFID3")
razonrfid4=os.environ.get("RAZON_RFID4")
razonrfid5=os.environ.get("RAZON_RFID5")
razonrfid6=os.environ.get("RAZON_RFID6")
razonrfid7=os.environ.get("RAZON_RFID7")
razonrfid8=os.environ.get("RAZON_RFID8")
razonrfid9=os.environ.get("RAZON_RFID9")
razonrfid10=os.environ.get("RAZON_RFID10")
razonrfid11=os.environ.get("RAZON_RFID11")
razonrfid12=os.environ.get("RAZON_RFID12")
razonrfid13=os.environ.get("RAZON_RFID13")
razonrfid14=os.environ.get("RAZON_RFID14")
razonrfid15=os.environ.get("RAZON_RFID15")
razonrfid16=os.environ.get("RAZON_RFID16")
razonrfid17=os.environ.get("RAZON_RFID17")
razonrfid18=os.environ.get("RAZON_RFID18")
razonrfid19=os.environ.get("RAZON_RFID19")
razonrfid20=os.environ.get("RAZON_RFID20")

acceso1=os.environ.get('URL_ACCESO1')
acceso2=os.environ.get('URL_ACCESO2')
acceso3=os.environ.get('URL_ACCESO3')
acceso4=os.environ.get('URL_ACCESO4')
acceso5=os.environ.get('URL_ACCESO5')
acceso6=os.environ.get('URL_ACCESO6')
acceso7=os.environ.get('URL_ACCESO7')
acceso8=os.environ.get('URL_ACCESO8')
acceso9=os.environ.get('URL_ACCESO9')
acceso10=os.environ.get('URL_ACCESO10')
acceso11=os.environ.get('URL_ACCESO11')
acceso12=os.environ.get('URL_ACCESO12')
acceso13=os.environ.get('URL_ACCESO13')
acceso14=os.environ.get('URL_ACCESO14')
acceso15=os.environ.get('URL_ACCESO15')
acceso16=os.environ.get('URL_ACCESO16')
acceso17=os.environ.get('URL_ACCESO17')
acceso18=os.environ.get('URL_ACCESO18')
acceso19=os.environ.get('URL_ACCESO19')
acceso20=os.environ.get('URL_ACCESO20')

accesodict = {'1':acceso1, '2':acceso2, '3':acceso3, '4':acceso4, '5':acceso5,
                '6':acceso6, '7':acceso7, '8':acceso8, '9':acceso9, '10':acceso10,
                '11':acceso11, '12':acceso12, '13':acceso13, '14':acceso14, '15':acceso15,
                '16':acceso16, '17':acceso17, '18':acceso18, '19':acceso19, '20':acceso20
                }
razondict = {'1':razon1, '2':razon2, '3':razon3, '4':razon4, '5':razon5,
            '6':razon6, '7':razon7, '8':razon8, '9':razon9, '10':razon10,
            '11':razon11, '12':razon12, '13':razon13, '14':razon14, '15':razon15,
                '16':razon16, '17':razon17, '18':razon18, '19':razon19, '20':razon20}

razondicthuellas = {'1':razonhuella1, '2':razonhuella2, '3':razonhuella3, '4':razonhuella4,  '5':razonhuella5,
                    '6':razonhuella6, '7':razonhuella7, '8':razonhuella8, '9':razonhuella9,  '10':razonhuella10,
                    '11':razonhuella11, '12':razonhuella12, '13':razonhuella13, '14':razonhuella14, '15':razonhuella15,
                    '16':razonhuella16, '17':razonhuella17, '18':razonhuella18, '19':razonhuella19, '20':razonhuella20}

razondictrfids = {'1':razonrfid1, '2':razonrfid2, '3':razonrfid3, '4':razonrfid4, '5':razonrfid5,
                    '6':razonrfid6, '7':razonrfid7, '8':razonrfid8, '9':razonrfid9, '10':razonrfid10,
                    '11':razonrfid11, '12':razonrfid12, '13':razonrfid13, '14':razonrfid14, '15':razonrfid15,
                    '16':razonrfid16, '17':razonrfid17, '18':razonrfid18, '19':razonrfid19, '20':razonrfid20}

# def aperturaconcedidawifi(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso):

#     try:
#         if accesodict[acceso]:
#             requests.get(url=f'{accesodict[acceso]}/on', timeout=3)
#             cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
#             VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razondict[acceso], contratof, cedulaf))
#             #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
#             connf.commit()
#     except:
#         cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
#         VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razondict[acceso]}', contratof, cedulaf))
#         #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
#         connf.commit()
#     finally:
#         pass

def aperturaConcedidaVigilante(vigilante_id, vigilante_nombre, unidad_id, unidad_nombre, fecha, hora, contrato, cursorf, connf, acceso, razon, personas):
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}-{razon}"
            requests.get(f'{accesodict[acceso]}/on', timeout=4)
            cursorf.execute('''INSERT INTO web_logs_vigilantes (vigilante_id, vigilante_nombre, unidad_id, unidad_nombre, fecha, hora, razon, contrato, personas)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);''', (vigilante_id, vigilante_nombre, unidad_id, unidad_nombre, fecha, hora, razonRegistrar, contrato, personas))
            connf.commit()
            if razon=='entrada':
                mensaje=f'Se ha dejado ingresar {personas} persona a su unidad' if (personas=='1' or personas=='0') else f"Se han dejado ingresar {personas} personas a su unidad"
            else:
                mensaje=f'{personas} persona ha salido de su unidad' if (personas=='1' or personas=='0') else f"{personas} personas han salido de su unidad"
            cursorf.execute('SELECT numero_telefonico FROM web_usuarios WHERE unidad_id=%s AND rol=%s',(unidad_id,'Propietario'))
            propietarios_unidad= cursorf.fetchall()
            for propietario in propietarios_unidad:
                try:
                    requests.get(f'https://api.callmebot.com/whatsapp.php?phone={NUMERO_BOT}&text=!sendto+{propietario[0][1:]}+{mensaje}&apikey={APIKEY_BOT}', timeout=5)
                except Exception as e:
                    print(f"{e} - fallo intentando enviar mensaje vigilante")
    except Exception as e:
        cursorf.execute('''INSERT INTO web_logs_vigilantes (vigilante_id, vigilante_nombre, unidad_id, unidad_nombre, fecha, hora, razon, contrato)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);''', (vigilante_id, vigilante_nombre, unidad_id, unidad_nombre, fecha, hora, f"fallo_{razonRegistrar}", contrato, personas))
        connf.commit()
        print(f"{e} - fallo intentando abrir el acceso {acceso} con permiso de vigilante")

def aperturaConcedidaVigilanteVisitante(vigilante_id, vigilante_nombre, nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, razon, horario_id, aperturasRealizadas, acompanantes, cedula_propietario, numero_propietario):
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(vigilante)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(vigilante)-{razon}"
            requests.get(f'{accesodict[acceso]}/on', timeout=4)
            cursorf.execute('UPDATE control_horarios_visitantes SET aperturas_hechas=%s WHERE horario_id=%s', (aperturasRealizadas+1,horario_id))
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado)
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            cursorf.execute('''INSERT INTO web_logs_visitantes (vigilante_id, vigilante_nombre, nombre, fecha, hora, razon, contrato, cedula_id, acompanantes, cedula_propietario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', (vigilante_id, vigilante_nombre, nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf, acompanantes, cedula_propietario))
            connf.commit()
            if razon=='entrada':
                mensaje=f"El invitado {nombref} acaba de ingresar por medio del sistema de vigilancia"
            else:
                mensaje=f"El invitado {nombref} acaba de salir por medio del sistema de vigilancia"
            try:
                requests.get(f'https://api.callmebot.com/whatsapp.php?phone={NUMERO_BOT}&text=!sendto+{numero_propietario[1:]}+{mensaje}&apikey={APIKEY_BOT}', timeout=5)
            except Exception as e:
                print(f"{e} - fallo intentando enviar mensaje vigilante visitante")
    except Exception as e:
        cursorf.execute('''INSERT INTO web_logs_visitantes (vigilante_id, vigilante_nombre, nombre, fecha, hora, razon, contrato, cedula_id, acompanantes, cedula_propietario)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', (vigilante_id, vigilante_nombre, nombref, fechaf, horaf, f"fallo_{razonRegistrar}", contratof, cedulaf, acompanantes, cedula_propietario))
        connf.commit()
        print(f"{e} - fallo intentando abrir el acceso {acceso} con permiso de vigilante")

def controlhorariovisitante(cursorf, connf, horario_id, razon):
    abrir=False
    cantidad_aperturas=0
    cursorf.execute('SELECT aperturas_hechas FROM control_horarios_visitantes WHERE horario_id=%s',(horario_id,))
    control_visitante= cursorf.fetchall()
    if not control_visitante:
        if razon=='entrada':
            cursorf.execute('''INSERT INTO control_horarios_visitantes (horario_id, aperturas_hechas) 
            VALUES (%s, %s)''', (horario_id, 0))
            connf.commit()
            abrir=True
    elif control_visitante[0][0]<2:
        if razon=='salida':
            cantidad_aperturas=control_visitante[0][0]
            abrir=True

    return abrir, cantidad_aperturas

def aperturaconcedidawifi(id_usuariof, cursorf, connf, acceso, cedulaf, nombref, fechaf, horaf, razon):
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Wifi)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Wifi)-{razon}"
            requests.get(f'{accesodict[acceso]}/on', timeout=2)
            cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, CONTRATO, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            # connf.commit()
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
    except:
        IdContador=0
        cursorf.execute('SELECT id FROM solicitud_aperturas ORDER BY id ASC')
        ids_peticiones_local= cursorf.fetchall()
        nro_ids_peticiones_local=len(ids_peticiones_local)
        if not ids_peticiones_local:
            idPeticion = 1
        else:
            for id_peticion_local in ids_peticiones_local:
                IdContador=IdContador+1
                if not id_peticion_local[0] == IdContador:
                    idPeticion=IdContador
                    break
            if nro_ids_peticiones_local == IdContador:
                idPeticion=IdContador+1

        if accesodict[acceso]:
            cursorf.execute('''INSERT INTO solicitud_aperturas (id, id_usuario, acceso, razon, estado, peticionInternet, feedback)
            VALUES (%s, %s, %s, %s, %s, %s, %s);''', (idPeticion, id_usuariof, acceso, razon, 0, 'f', 'f'))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            connf.commit()

def aperturaconcedidawifivisitante(id_usuariof, cursorf, connf, acceso, cedulaf, nombref, fechaf, horaf, razon, horario_id, aperturasRealizadas):
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Wifi)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Wifi)-{razon}"
            requests.get(f'{accesodict[acceso]}/on', timeout=2)
            cursor.execute('UPDATE control_horarios_visitantes SET aperturas_hechas=%s WHERE horario_id=%s', (aperturasRealizadas+1,horario_id))
            cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, CONTRATO, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            # connf.commit()
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()        
    except:
        IdContador=0
        cursorf.execute('SELECT id FROM solicitud_aperturas ORDER BY id ASC')
        ids_peticiones_local= cursorf.fetchall()
        nro_ids_peticiones_local=len(ids_peticiones_local)
        if not ids_peticiones_local:
            idPeticion = 1
        else:
            for id_peticion_local in ids_peticiones_local:
                IdContador=IdContador+1
                if not id_peticion_local[0] == IdContador:
                    idPeticion=IdContador
                    break
            if nro_ids_peticiones_local == IdContador:
                idPeticion=IdContador+1

        if accesodict[acceso]:
            cursorf.execute('''INSERT INTO solicitud_aperturas (id, id_usuario, acceso, razon, estado, peticionInternet, feedback)
            VALUES (%s, %s, %s, %s, %s, %s, %s);''', (idPeticion, id_usuariof, acceso, razon, 0, 'f', 'f'))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            connf.commit()


def aperturaconcedidahuella(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, razon):

    try:
        if accesodict[acceso]:
            razonRegistrar=razondicthuellas[acceso] if (razon in razondicthuellas[acceso].lower()) else f"{razondicthuellas[acceso]}-{razon}"
            requests.get(url=f'{accesodict[acceso]}/onrh', timeout=3)
            cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
    except:
        cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
        VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razonRegistrar}', contratof, cedulaf))
        #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
        connf.commit()
    finally:
        pass

def aperturaconcedidarfid(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso):

    try:
        if accesodict[acceso]:
            razonRegistrar=razondictrfids[acceso] if (razon in razondictrfids[acceso].lower()) else f"{razondictrfids[acceso]}-{razon}"
            requests.get(url=f'{accesodict[acceso]}/onrh', timeout=3)
            cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
    except:
        cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
        VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razonRegistrar}', contratof, cedulaf))
        #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
        connf.commit()
    finally:
        pass

def aperturaconcedidabluetooth(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, razon):

    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Bluetooth)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Bluetooth)-{razon}"
            requests.get(f'{accesodict[acceso]}/on', timeout=2)
            cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, CONTRATO, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            # connf.commit()
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
    except:
        cursorf.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula_id)
        VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razonRegistrar}', contratof, cedulaf))
        #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
        connf.commit()
    finally:
        pass

def aperturaconcedidabluetoothvisitante(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, razon, horario_id, aperturasRealizadas, acompanantes, cedula_propietario, numero_propietario):

    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(bluetooth)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(bluetooth)-{razon}"
            requests.get(f'{accesodict[acceso]}/on', timeout=4)
            cursorf.execute('UPDATE control_horarios_visitantes SET aperturas_hechas=%s WHERE horario_id=%s', (aperturasRealizadas+1,horario_id))
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado)
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            cursorf.execute('''INSERT INTO web_logs_visitantes (nombre, fecha, hora, razon, contrato, cedula_id, acompanantes, cedula_propietario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf, acompanantes, cedula_propietario))
            connf.commit()
            if razon=='entrada':
                mensaje=f"El invitado {nombref} acaba de ingresar por medio de bluetooth"
            else:
                mensaje=f"El invitado {nombref} acaba de salir por medio de bluetooth"
            requests.get(f'https://api.callmebot.com/whatsapp.php?phone={NUMERO_BOT}&text=!sendto+{numero_propietario[1:]}+{mensaje}&apikey={APIKEY_BOT}', timeout=5)
    except Exception as e:
        cursorf.execute('''INSERT INTO web_logs_visitantes (vigilante_id, vigilante_nombre, nombre, fecha, hora, razon, contrato, cedula_id, acompanantes, cedula_propietario)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f"fallo_{razonRegistrar}", contratof, cedulaf, acompanantes, cedula_propietario))
        connf.commit()
        print(f"{e} - fallo intentando abrir el acceso {acceso} por bluetooth")

def aperturadenegada(cursorf, connf, acceso):
    # cursorf.execute('''UPDATE led SET onoff=2 WHERE onoff=0;''')
    # connf.commit()
    try:
        requests.get(url=f'{accesodict[acceso]}/off', timeout=3)
    except:
        print("fallo en peticion http")
    finally:
        pass

def invertir_uuid(uuid_beacon):
    uuidInvertido=''
    cont=36
    for char in range(1,17):
        uuidInvertido=uuidInvertido+uuid_beacon[cont-2:cont]
        cont=cont-2
        if char==4:
            uuidInvertido+='-'
        if char==6:
            cont=cont-1
            uuidInvertido+='-'
        if char==8:
            cont=cont-1
            uuidInvertido+='-'
        if char==10:
            cont=cont-1
            uuidInvertido+='-'
        if char==12:
            cont=cont-1
    return uuidInvertido

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/seguricel_ping":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
        # else:
        #     print("cerrado")
        #     webServer.shutdown()
        
        # peticion=self.path[1::].split("/")
        # print(f"peticion = {peticion}")
        # self.send_header("Content-type", "utf-8")
        # self.end_headers()
        #self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))

        

    def do_POST(self):
        peticion=self.path[1::].split("/")

        if len(peticion) == 2 and peticion[1] == "cerrandoacceso":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
            acceso_solicitud, _ = peticion
            cursor.execute('UPDATE accesos_abiertos SET estado=%s WHERE acceso=%s', ('t', acceso_solicitud))
            conn.commit()
        
        if len(peticion) == 2 and peticion[1] == "noregistrado":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
            acceso_solicitud, _ = peticion
            aperturadenegada(cursor, conn, acceso_solicitud)

        if len(peticion) == 6 and peticion[5] == "seguricel_wifi_vigilante":
            # self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))
            id_usuario, codigo_unidad, personas, acceso_solicitud, razonApertura, _ = peticion
            # print(codigo_unidad)
            # print(personas)
            # print(acceso_solicitud)
            # print(razonApertura)

            cursor.execute("SELECT id, nombre FROM web_usuarios where telegram_id=%s", (id_usuario,))
            datosVigilante = cursor.fetchall()
            cursor.execute("SELECT id, nombre FROM web_unidades where codigo=%s", (codigo_unidad,))
            datosUnidad = cursor.fetchall()
            
            # print(datosUnidad)
            if len(datosUnidad)!=0 and len(datosVigilante)!=0:
                tz = pytz.timezone('America/Caracas')
                caracas_now = datetime.now(tz)
                hora=str(caracas_now)[11:19]
                fecha=str(caracas_now)[:10]
                unidad_id=datosUnidad[0][0]
                unidad_nombre=datosUnidad[0][1]
                vigilante_id=datosVigilante[0][0]
                vigilante_nombre=datosVigilante[0][1]
                
                aperturaConcedidaVigilante(vigilante_id, vigilante_nombre, unidad_id, unidad_nombre, fecha, hora, CONTRATO, cursor, conn, acceso_solicitud, razonApertura, personas)
                self.send_response(200)
                self.send_header("Content-type", "utf-8")
                self.end_headers()
            else:
                aperturadenegada(cursor, conn, acceso_solicitud)
                self.send_response(400)
                self.send_header("Content-type", "utf-8")
                self.end_headers()
        
        if len(peticion) == 6 and peticion[5] == "seguricel_wifi_vigilante_invitado":
            # self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))
            id_usuario, horario_id, acompanantes, acceso_solicitud, razonApertura, _ = peticion
            # print(codigo_unidad)
            # print(personas)
            # print(acceso_solicitud)
            # print(razonApertura)

            cursor.execute("SELECT id, nombre FROM web_usuarios where telegram_id=%s", (id_usuario,))
            datosVigilante = cursor.fetchall()
            cursor.execute("SELECT usuario, fecha_entrada, fecha_salida, entrada, salida FROM web_horariospermitidos where id=%s", (int(horario_id),))
            datosHorario = cursor.fetchall()


            if len(datosHorario)!=0:
                cursor.execute("SELECT cedula, nombre, cedula_propietario FROM web_usuarios where id=%s", (int(datosHorario[0][0]),))
                datosInvitado = cursor.fetchall()
                cursor.execute("SELECT numero_telefonico FROM web_usuarios where cedula=%s", (datosInvitado[0][2],))
                datosPropietario = cursor.fetchall()
                if len(datosInvitado)!=0 and len(datosPropietario)!=0:
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    horahoy = caracas_now.time()
                    fechahoy = caracas_now.date()
                    for _, fecha_entrada, fecha_salida, entrada, salida in datosHorario:
                        if (fechahoy==fecha_entrada and horahoy>=entrada) or (fechahoy > fecha_entrada and fechahoy<fecha_salida) or (fechahoy==fecha_salida and horahoy<=salida):
                            permitir, aperturasRealizadas = controlhorariovisitante(cursor, conn, horario_id, razonApertura)
                            if permitir:
                                invitado_cedula=datosInvitado[0][0]
                                invitado_nombre=datosInvitado[0][1]
                                vigilante_id=datosVigilante[0][0]
                                vigilante_nombre=datosVigilante[0][1]
                                fecha=str(caracas_now)[:10]
                                
                                aperturaConcedidaVigilanteVisitante(vigilante_id, vigilante_nombre, invitado_nombre, fecha, horahoy, CONTRATO, invitado_cedula, cursor, conn, acceso_solicitud, razonApertura, horario_id, aperturasRealizadas, acompanantes, datosInvitado[0][2], datosPropietario[0][0])
                                self.send_response(200)
                                self.send_header("Content-type", "utf-8")
                                self.end_headers()
                            else:
                                aperturadenegada(cursor, conn, acceso_solicitud)
                                self.send_response(200)
                                self.send_header("Content-type", "utf-8")
                                self.end_headers()  
                        else:
                            aperturadenegada(cursor, conn, acceso_solicitud)
                            self.send_response(200)
                            self.send_header("Content-type", "utf-8")
                            self.end_headers()
                else:
                    aperturadenegada(cursor, conn, acceso_solicitud)
                    self.send_response(200)
                    self.send_header("Content-type", "utf-8")
                    self.end_headers()
            else:
                aperturadenegada(cursor, conn, acceso_solicitud)
                self.send_response(400)
                self.send_header("Content-type", "utf-8")
                self.end_headers()

        if len(peticion) == 4 and peticion[3] == "seguricel_wifi_activo":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
            # self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))

            id_usuario, acceso_solicitud, razonApertura, _ = peticion
            #print(id_usuario)
            #print(acceso_solicitud)

            diasusuario = []
            etapadia=0
            etapadiaapertura=0
            cantidaddias = 0
            contadoraux = 0
            cursor.execute("SELECT cedula, nombre, wifi, telegram_id, rol, id FROM web_usuarios where telegram_id=%s", (id_usuario,))
            datosUsuario = cursor.fetchall()
            #print(datosUsuario)
            if len(datosUsuario)!=0:
                cedula=datosUsuario[0][0]
                nombre=datosUsuario[0][1]
                permisoAperturaWifi = datosUsuario[0][2]
                idUsuario = datosUsuario[0][3]
                rol=datosUsuario[0][4]
                usuario_id=datosUsuario[0][5]
                cursor.execute('SELECT id, fecha_entrada, fecha_salida, entrada, salida, dia FROM web_horariospermitidos where usuario=%s', (usuario_id,))
                horarios_permitidos = cursor.fetchall()
                if (horarios_permitidos != [] and permisoAperturaWifi == True and rol=='Secundario'):
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    dia = caracas_now.weekday()
                    diahoy = dias_semana[dia]
                    for _, _, _, entrada, salida, dia in horarios_permitidos:
                        diasusuario.append(dia)
                    cantidaddias = diasusuario.count(dia)
                    for _, _, _, entrada, salida, dia in horarios_permitidos:
                        if 'Siempre' in diasusuario:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                            aperturaconcedidawifi(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura)
                            etapadiaapertura=1
                        elif dia==diahoy and cantidaddias==1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidawifi(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidawifi(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                        elif dia==diahoy and cantidaddias>1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidawifi(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidawifi(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                                    #print('fuera de horario')
                    if etapadia==0 and etapadiaapertura==0:
                        aperturadenegada(cursor, conn, acceso_solicitud)
                        #print('Dia no permitido')
                elif rol=='Propietario' and permisoAperturaWifi == True:
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    hora=str(caracas_now)[11:19]
                    horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                    fecha=str(caracas_now)[:10]
                    aperturaconcedidawifi(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura)
                elif (horarios_permitidos != [] and permisoAperturaWifi == True and rol=='Visitante'):
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    horahoy = caracas_now.time()
                    fechahoy = caracas_now.date()
                    for horario_id, fecha_entrada, fecha_salida, entrada, salida, _ in horarios_permitidos:
                        if (fechahoy==fecha_entrada and horahoy>=entrada) or (fechahoy > fecha_entrada and fechahoy<fecha_salida) or (fechahoy==fecha_salida and horahoy<=salida):
                            permitir, aperturasRealizadas = controlhorariovisitante(cursor, conn, horario_id, razonApertura)
                            if permitir:
                                fecha=str(caracas_now)[:10]
                                aperturaconcedidawifivisitante(idUsuario, cursor, conn, acceso_solicitud, cedula, nombre, fecha, horahoy, razonApertura, horario_id, aperturasRealizadas)
                            else:
                                aperturadenegada(cursor, conn, acceso_solicitud)  
                        else:
                            aperturadenegada(cursor, conn, acceso_solicitud)
                else:
                    aperturadenegada(cursor, conn, acceso_solicitud)
                # if horarios_permitidos == []:
                #     aperturadenegada(cursor, conn, acceso_solicitud)
                #     #print('este usuario no tiene horarios establecidos')
                diasusuario=[]
            else:
                aperturadenegada(cursor, conn, acceso_solicitud)

        if len(peticion) == 4 and peticion[3] == "seguricel_bluetooth_activo":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
            # self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))

            uuid_usuario, acceso_solicitud, razonApertura, _ = peticion
            #print(id_usuario)
            #print(acceso_solicitud)
            #uuid_usuario = invertir_uuid(uuid_usuario)

            diasusuario = []
            etapadia=0
            etapadiaapertura=0
            cantidaddias = 0
            contadoraux = 0
            cursor.execute("SELECT cedula, nombre, bluetooth, rol, id, cedula_propietario FROM web_usuarios where entrada_beacon_uuid=%s", (uuid_usuario,))
            datosUsuario = cursor.fetchall()
            razonApertura='entrada'
            if not datosUsuario:
                cursor.execute("SELECT cedula, nombre, bluetooth, rol, id, cedula_propietario FROM web_usuarios where salida_beacon_uuid=%s", (uuid_usuario,))
                datosUsuario = cursor.fetchall()
                razonApertura='salida'
            #print(datosUsuario)
            if len(datosUsuario)!=0:
                cedula=datosUsuario[0][0]
                nombre=datosUsuario[0][1]
                permisoAperturaBluetooth = datosUsuario[0][2]
                rol=datosUsuario[0][3]
                usuario_id=datosUsuario[0][4]
                cedula_propietario=datosUsuario[0][5]
                cursor.execute('SELECT id, fecha_entrada, fecha_salida, entrada, salida, dia, acompanantes FROM web_horariospermitidos where usuario=%s', (usuario_id,))
                horarios_permitidos = cursor.fetchall()
                if horarios_permitidos != [] and permisoAperturaBluetooth == True and rol=='Secundario':
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    dia = caracas_now.weekday()
                    diahoy = dias_semana[dia]
                    for _, _, _, entrada, salida, dia, _ in horarios_permitidos:
                        diasusuario.append(dia)
                    cantidaddias = diasusuario.count(dia)
                    for _, _, _, entrada, salida, dia, _ in horarios_permitidos:
                        if 'Siempre' in diasusuario:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                            aperturaconcedidabluetooth(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                            etapadiaapertura=1
                        elif dia==diahoy and cantidaddias==1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidabluetooth(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidabluetooth(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                        elif dia==diahoy and cantidaddias>1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidabluetooth(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                                    aperturaconcedidabluetooth(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                                    #print('fuera de horario')
                    if etapadia==0 and etapadiaapertura==0:
                        aperturadenegada(cursor, conn, acceso_solicitud)
                        #print('Dia no permitido')
                elif rol=='Propietario' and permisoAperturaBluetooth == True:
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    hora=str(caracas_now)[11:19]
                    horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                    fecha=str(caracas_now)[:10]
                    #aperturaconcedidawifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud)
                    aperturaconcedidabluetooth(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                elif (horarios_permitidos != [] and permisoAperturaBluetooth == True and rol=='Visitante'):
                    cursor.execute("SELECT numero_telefonico FROM web_usuarios where cedula=%s", (cedula_propietario,))
                    datosPropietario = cursor.fetchall()
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    horahoy = caracas_now.time()
                    fechahoy = caracas_now.date()
                    for horario_id, fecha_entrada, fecha_salida, entrada, salida, _, acompanantes in horarios_permitidos:
                        if (fechahoy==fecha_entrada and horahoy>=entrada) or (fechahoy > fecha_entrada and fechahoy<fecha_salida) or (fechahoy==fecha_salida and horahoy<=salida):
                            permitir, aperturasRealizadas = controlhorariovisitante(cursor, conn, horario_id, razonApertura)
                            if permitir and len(datosPropietario):
                                fecha=str(caracas_now)[:10]
                                aperturaconcedidabluetoothvisitante(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura, horario_id, aperturasRealizadas, acompanantes, cedula_propietario, datosPropietario[0][0])
                            else:
                                aperturadenegada(cursor, conn, acceso_solicitud)  
                        else:
                            aperturadenegada(cursor, conn, acceso_solicitud)
                else:
                    aperturadenegada(cursor, conn, acceso_solicitud)
                    #print('este usuario no tiene horarios establecidos')
                diasusuario=[]
            else:
                aperturadenegada(cursor, conn, acceso_solicitud)

        if len(peticion) == 4 and peticion[3] == "seguricel_captahuella_activo":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
            # self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))

            id_suprema, acceso_solicitud, razonApertura, _ = peticion
            id_suprema = id_suprema[6:]+id_suprema[4:6]+id_suprema[2:4]+id_suprema[0:2]
            id_suprema = int(id_suprema, 16)
            #print(id_suprema)
            #print(acceso_solicitud)

            diasusuario = []
            etapadia=0
            etapadiaapertura=0
            cantidaddias = 0
            contadoraux = 0
            cursor.execute("SELECT cedula FROM web_huellas where id_suprema=%s", (id_suprema,))
            datosusuario_huella = cursor.fetchall()
            #print(datosUsuario)
            if len(datosusuario_huella)!=0:
                cursor.execute("SELECT cedula, nombre, captahuella, rol, id FROM web_usuarios where cedula=%s", (datosusuario_huella[0][0],))
                datosUsuario = cursor.fetchall()
                cedula=datosUsuario[0][0]
                nombre=datosUsuario[0][1]
                permisoAperturaHuella = datosUsuario[0][2]
                rol=datosUsuario[0][3]
                usuario_id=datosUsuario[0][4]
                cursor.execute('SELECT * FROM web_horariospermitidos where usuario=%s', (usuario_id,))
                horarios_permitidos = cursor.fetchall()
                if horarios_permitidos != [] and permisoAperturaHuella == True and rol=='Secundario':
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    dia = caracas_now.weekday()
                    diahoy = dias_semana[dia]
                    for entrada, salida, _, dia in horarios_permitidos:
                        diasusuario.append(dia)
                    cantidaddias = diasusuario.count(dia)
                    for entrada, salida, _, dia in horarios_permitidos:
                        if 'Siempre' in diasusuario:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            aperturaconcedidahuella(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                            etapadiaapertura=1
                        elif dia==diahoy and cantidaddias==1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    aperturaconcedidahuella(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    aperturaconcedidahuella(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                        elif dia==diahoy and cantidaddias>1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    aperturaconcedidahuella(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    aperturaconcedidahuella(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                                    #print('fuera de horario')
                    if etapadia==0 and etapadiaapertura==0:
                        aperturadenegada(cursor, conn, acceso_solicitud)
                        #print('Dia no permitido')
                elif rol=='Propietario' and permisoAperturaHuella == True:
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    hora=str(caracas_now)[11:19]
                    horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                    fecha=str(caracas_now)[:10]
                    aperturaconcedidahuella(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                else:
                    aperturadenegada(cursor, conn, acceso_solicitud)
                # if horarios_permitidos == []:
                #     aperturadenegada(cursor, conn, acceso_solicitud)
                #     #print('este usuario no tiene horarios establecidos')
                diasusuario=[]
            else:
                aperturadenegada(cursor, conn, acceso_solicitud)

        if len(peticion) == 4 and peticion[3] == "seguricel_rfid_activo":
            self.send_response(200)
            self.send_header("Content-type", "utf-8")
            self.end_headers()
            # self.wfile.write(bytes(f"{self.path[1::]}", "utf-8"))
            epc, acceso_solicitud, razonApertura, _ = peticion
            diasusuario = []
            etapadia=0
            etapadiaapertura=0
            cantidaddias = 0
            contadoraux = 0
            cursor.execute("SELECT cedula FROM web_tagsrfid where epc=%s", (epc,))
            datosusuario_rfid = cursor.fetchall()
            #print(datosusuario)
            if len(datosusuario_rfid)!=0:
                cursor.execute("SELECT cedula, nombre, rfid, rol, id  FROM web_usuarios where cedula=%s", (datosusuario_rfid[0][0],))
                datosUsuario = cursor.fetchall()
                cedula=datosUsuario[0][0]
                nombre=datosUsuario[0][1]
                permisoAperturaRFID = datosUsuario[0][2]
                rol=datosUsuario[0][3]
                usuario_id=datosUsuario[0][4]
                cursor.execute('SELECT * FROM web_horariospermitidos where usuario=%s', (usuario_id,))
                horarios_permitidos = cursor.fetchall()
                if horarios_permitidos != [] and permisoAperturaRFID == True and rol=='Secundario':
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    dia = caracas_now.weekday()
                    diahoy = dias_semana[dia]
                    for entrada, salida, _, dia in horarios_permitidos:
                        diasusuario.append(dia)
                    cantidaddias = diasusuario.count(dia)
                    for entrada, salida, _, dia in horarios_permitidos:
                        if 'Siempre' in diasusuario:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            aperturaconcedidarfid(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                            etapadiaapertura=1
                        elif dia==diahoy and cantidaddias==1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    aperturaconcedidarfid(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    aperturaconcedidarfid(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                else:
                                    aperturadenegada(cursor, conn, acceso_solicitud)
                                    #print('fuera de horario')
                        elif dia==diahoy and cantidaddias>1:
                            hora=str(caracas_now)[11:19]
                            horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                            fecha=str(caracas_now)[:10]
                            etapadia=1
                            if entrada<salida:
                                if horahoy >= entrada and horahoy <= salida:
                                    #print('entrada concedida')
                                    aperturaconcedidarfid(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                            if entrada>salida:
                                if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                    #print('entrada concedida')
                                    aperturaconcedidarfid(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                                    etapadiaapertura=1
                                    contadoraux=0
                                else:
                                    contadoraux = contadoraux+1
                                    if contadoraux == cantidaddias:
                                        aperturadenegada(cursor, conn, acceso_solicitud)
                                        contadoraux=0
                                    #print('fuera de horario')
                    if etapadia==0 and etapadiaapertura==0:
                        aperturadenegada(cursor, conn, acceso_solicitud)
                        #print('Dia no permitido')
                elif rol=='Propietario' and permisoAperturaRFID == True:
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    hora=str(caracas_now)[11:19]
                    horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                    fecha=str(caracas_now)[:10]
                    aperturaconcedidarfid(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, razonApertura)
                else:
                    aperturadenegada(cursor, conn, acceso_solicitud)    
                # if horarios_permitidos == []:
                #     aperturadenegada(cursor, conn, acceso_solicitud)
                #     #print('este usuario no tiene horarios establecidos')
                diasusuario=[]
            else:
                aperturadenegada(cursor, conn, acceso_solicitud)


if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    while True:

        t11=time.perf_counter()
        while total<=5:
            t22=time.perf_counter()
            total=t22-t11
        total=0
        try:
            conn = psycopg2.connect(
                database=os.environ.get("DATABASE"),
                user=os.environ.get("USERDB"),
                password=os.environ.get("PASSWORD"),
                host=os.environ.get("HOST"),
                port=os.environ.get("PORT")
            )
            cursor = conn.cursor()
            webServer.serve_forever()
            print("fallo server")
        except (Exception, psycopg2.Error, KeyboardInterrupt) as error:
            print("fallo en hacer las consultas")
            total=0
        finally:
            print("se ha cerrado la conexion a la base de datos")
            print("Server stopped.")
            if conn:
                cursor.close()
                conn.close()
                total=0
            webServer.server_close()