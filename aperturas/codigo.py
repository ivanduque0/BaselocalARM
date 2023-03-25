import time
import psycopg2
import os
import pytz
from datetime import datetime
import requests
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/BaselocalARMsindocker/.env.manager')
load_dotenv(dotenv_path=dotenv_path)

dias_semana = ("Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo")
ultimahora = datetime.strptime('23:59:59', '%H:%M:%S').time()
primerahora = datetime.strptime('00:00:00', '%H:%M:%S').time()
total=0
CONTRATO=os.environ.get("CONTRATO")
URL_API = os.environ.get("URL_API")
TIEMPO_MAX_ENCOLAMIENTO = float(os.environ.get("TIEMPO_MAX_ENCOLAMIENTO"))
conn=None
cursor=None

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

def controlhorariovisitante(cursorf, connf, horario_id, razon):
    abrir=False
    cantidad_aperturas=0
    cursorf.execute('SELECT aperturas_hechas FROM control_horarios_visitantes WHERE horario_id=%s',(horario_id,))
    control_visitante= cursorf.fetchall()
    if not control_visitante:
        if razon=='entrada':
            cursorf.execute('''INSERT INTO control_horarios_visitantes (horario_id, aperturas_hechas) 
            VALUES (%s, %s)''', (horario_id, 0))
            conn.commit()
            abrir=True
    elif control_visitante[0][0]<2:
        if razon=='salida':
            cantidad_aperturas=control_visitante[0][0]
            abrir=True

    return abrir, cantidad_aperturas

def aperturaConcedidaInternet(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, id_solicitud, razon, abriendo):
    
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Internet)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Internet)-{razon}"
            if abriendo==False:
                requests.get(f'{accesodict[acceso]}/on', timeout=5)
                cursorf.execute('UPDATE solicitud_aperturas SET abriendo=%s WHERE id=%s;', ('t', id_solicitud))
                connf.commit()
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
            requests.put(url=f'{URL_API}aperturasusuarioapi/{id_solicitud}/{contratof}/', auth=('BaseLocal_access', 'S3gur1c3l_local@'), timeout=5)
            cursorf.execute('UPDATE solicitud_aperturas SET estado=%s WHERE id=%s;', (1, id_solicitud))
            connf.commit()
            cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf))
            connf.commit()
    except Exception as e:
        print(f"{e} - fallo intentando aperturar desde internet en la peticion con id {id_solicitud}")
        # cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
        # VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razondict[acceso]}-Internet', contratof, cedulaf))
        # #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
        # connf.commit()
    # finally:
    #     pass

def aperturaConcedidaInternetVisitante(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, id_solicitud, razon, horario_id, aperturasRealizada, abriendo):
    
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Internet)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Internet)-{razon}"
            if abriendo==False:
                requests.get(f'{accesodict[acceso]}/on', timeout=5)
                cursorf.execute('UPDATE solicitud_aperturas SET abriendo=%s WHERE id=%s;', ('t', id_solicitud))
                connf.commit()
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
            requests.put(url=f'{URL_API}aperturasusuarioapi/{id_solicitud}/{contratof}/', auth=('BaseLocal_access', 'S3gur1c3l_local@'), timeout=5)
            cursorf.execute('UPDATE solicitud_aperturas SET estado=%s WHERE id=%s;', (1, id_solicitud))
            connf.commit()
            cursor.execute('UPDATE control_horarios_visitantes SET aperturas_hechas=%s WHERE horario_id=%s', (aperturasRealizadas+1,horario_id))
            cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf))
            connf.commit()

    except Exception as e:
        print(f"{e} - fallo intentando aperturar desde internet en la peticion con id {id_solicitud}")
        # cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
        # VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razondict[acceso]}-Internet', contratof, cedulaf))
        # #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
        # connf.commit()
    # finally:
    #     pass

def aperturaConcedidaWifi(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, id_solicitud, razon, abriendo):
    
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Wifi)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Wifi)-{razon}"
            if abriendo==False:
                requests.get(f'{accesodict[acceso]}/on', timeout=5)
                cursorf.execute('UPDATE solicitud_aperturas SET abriendo=%s WHERE id=%s;', ('t', id_solicitud))
                connf.commit()
            cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            connf.commit()
            cursorf.execute('UPDATE solicitud_aperturas SET estado=%s WHERE id=%s;', (1, id_solicitud))
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
    except Exception as e:
        print(f"{e} - fallo intentando aperturar desde wifi en la peticion con id {id_solicitud}")
        # cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
        # VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, f'fallo_{razondict[acceso]}-Wifi', contratof, cedulaf))
        # #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
        # connf.commit()
    # finally:
    #     pass
	
    # EN CASO DE USAR UN SERVIDOR LOCAL COMUN Y QUERER ACTIVAR CON SOLO UN ESP8266 EN CAMPO Y VARIOS ESP01, SE DEBE USAR ESTO
    # url = "http://tesis-reconocimiento-facial.herokuapp.com/apertura/"
    # dataa = {'contrato': CONTRATO, 'acceso': acceso}
    # requests.post(url, data=dataa)
    # r = requests.get('http://tesis-reconocimiento-facial.herokuapp.com/apertura$
    # jsonget = r.json()[0]
    # contrato = jsonget['contrato']
    # acceso = jsonget['acceso']
    # while contrato != CONTRATO or acceso == 'no':
    #     requests.post(url, data=dataa)
    #     r = requests.get('http://tesis-reconocimiento-facial.herokuapp.com/aper$
    #     jsonget = r.json()[0]
    #     contrato = jsonget['contrato']
    #     acceso = jsonget['acceso']
    # dataa = {'contrato': 'no', 'acceso': 'no'}
    # requests.post(url, data=dataa)

def aperturaConcedidaWifiVisitante(nombref, fechaf, horaf, contratof, cedulaf, cursorf, connf, acceso, id_solicitud, razon, horario_id, aperturasRealizadas, abriendo):
    
    try:
        if accesodict[acceso]:
            razonRegistrar=f"{razondict[acceso]}(Wifi)" if (razon in razondict[acceso].lower()) else f"{razondict[acceso]}(Wifi)-{razon}"
            if abriendo==False:
                requests.get(f'{accesodict[acceso]}/on', timeout=5)
                cursorf.execute('UPDATE solicitud_aperturas SET abriendo=%s WHERE id=%s;', ('t', id_solicitud))
                connf.commit()
            cursor.execute('UPDATE control_horarios_visitantes SET aperturas_hechas=%s WHERE horario_id=%s', (aperturasRealizadas+1,horario_id))
            cursorf.execute('''INSERT INTO web_logs_usuarios (nombre, fecha, hora, razon, contrato, cedula_id)
            VALUES (%s, %s, %s, %s, %s, %s);''', (nombref, fechaf, horaf, razonRegistrar, contratof, cedulaf))
            #cursorf.execute('''UPDATE led SET onoff=1 WHERE onoff=0;''')
            connf.commit()
            cursorf.execute('UPDATE solicitud_aperturas SET estado=%s WHERE id=%s;', (1, id_solicitud))
            cursorf.execute('''INSERT INTO accesos_abiertos (cedula, acceso, fecha, hora, estado) 
            VALUES (%s, %s, %s, %s, %s)''', (cedulaf, acceso, fechaf, horaf, 'f'))
            connf.commit()
    except Exception as e:
        print(f"{e} - fallo intentando aperturar desde wifi en la peticion con id {id_solicitud}")  

def aperturadenegada(cursorf, connf, acceso, id_solicitud):
    # cursorf.execute('''UPDATE led SET onoff=2 WHERE onoff=0;''')
    # connf.commit()
    try:
        requests.get(f'{accesodict[acceso]}/off', timeout=2)
        cursorf.execute('UPDATE solicitud_aperturas SET estado=%s, feedback=%s WHERE id=%s;', (1,'t', id_solicitud))
        connf.commit()
        requests.put(url=f'{URL_API}aperturasusuarioapi/{id_solicitud}/{CONTRATO}/', auth=('BaseLocal_access', 'S3gur1c3l_local@'), timeout=5)
    except Exception as e:
        
        print(f"{e} - fallo en peticion para denegar apertura")
    # finally:
    #     pass  

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

        while True:
            try:
                aperturas_solicitadas = requests.get(url=f'{URL_API}aperturascontratoapi/{CONTRATO}/', auth=('BaseLocal_access', 'S3gur1c3l_local@'), timeout=3).json()
                if len(aperturas_solicitadas):
                    tz = pytz.timezone('America/Caracas')
                    caracas_now = datetime.now(tz)
                    hora=str(caracas_now)[11:19]
                    hora_peticion = datetime.strptime(hora, '%H:%M:%S').time()
                    hora_hora=int(hora[:2])
                    hora_minuto=int(hora[3:5])
                    fecha=str(caracas_now)[:10]
                    for apertura in aperturas_solicitadas:
                        #print(dt['contrato'])
                        solicitud_hora_completa = apertura['hora']
                        solicitud_hora=int(solicitud_hora_completa[:2])
                        solicitud_minuto=int(solicitud_hora_completa[3:5])
                        diferencia_horas=hora_hora-solicitud_hora
                        diferencia_minutos=hora_minuto-solicitud_minuto
                        solicitud_id=apertura['id']
                        id_usuario = apertura['codigo_usuario']
                        solicitud_acceso=apertura['acceso']
                        razon=apertura['razon']
                        feedbackPeticion=apertura['feedback']
                        cursor.execute('SELECT * FROM solicitud_aperturas WHERE id=%s',(solicitud_id,))
                        aperturas_local_existente= cursor.fetchall()
                        if not aperturas_local_existente:
                            if apertura['fecha'] == fecha and diferencia_horas==0 and (diferencia_minutos >= -1 or diferencia_minutos <= 2):
                                    cursor.execute('''INSERT INTO solicitud_aperturas (id, id_usuario, acceso, razon,  estado, peticionInternet, feedback, abriendo, fecha, hora)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (solicitud_id, id_usuario, solicitud_acceso, razon, 0, 't', 'f', 'f', fecha, hora_peticion))
                                    conn.commit()
                            else:   
                                cursor.execute('''INSERT INTO solicitud_aperturas (id, id_usuario, acceso, razon, estado, peticionInternet, feedback, abriendo)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (solicitud_id, id_usuario, solicitud_acceso, razon, 1, 't', 't', 't'))
                                conn.commit()
                        elif aperturas_local_existente and feedbackPeticion:
                            cursor.execute('UPDATE solicitud_aperturas SET feedback=%s WHERE id=%s', ('t',solicitud_id))
                            conn.commit()
            except Exception as error:
                print(f"{error} - fallo en api solicitud de aperturas")

            cursor.execute('SELECT id, id_usuario, acceso, estado, peticionInternet, razon, abriendo, fecha, hora FROM solicitud_aperturas')
            aperturas_local= cursor.fetchall()

            if len(aperturas_local):
                for aperturalocal in aperturas_local:
                    estado_solicitud=aperturalocal[3]
                    #si es igual a 0 es porque aun no ha sido procesada la solicitud
                    #de apertura
                    if estado_solicitud == 0:
                        tz = pytz.timezone('America/Caracas')
                        caracas_now = datetime.now(tz)
                        diasusuario = []
                        etapadia=0
                        etapadiaapertura=0
                        cantidaddias = 0
                        contadoraux = 0
                        codigo_usuario = aperturalocal[1]
                        acceso_solicitud=aperturalocal[2]
                        id_solicitud=aperturalocal[0]
                        peticion_internet=aperturalocal[4]
                        razonApertura=aperturalocal[5]
                        abriendo=aperturalocal[6]
                        fecha=aperturalocal[7]
                        hora=aperturalocal[8]
                        cursor.execute("SELECT cedula, nombre, internet, wifi, rol, id FROM web_usuarios where telegram_id=%s", (codigo_usuario,))
                        datosUsuario = cursor.fetchall()
                        
                        hora_solicitud_str = hora.isoformat()
                        hora_solicitud = datetime.strptime(hora_solicitud_str, "%H:%M:%S")
                        hora_actual_str = str(caracas_now)[11:19]
                        hora_actual = datetime.strptime(hora_actual_str, "%H:%M:%S")
                        diferencia_horas = hora_actual - hora_solicitud
                        segundos_transcurridos=diferencia_horas.total_seconds()
                        #print(datosUsuario)
                        if len(datosUsuario)!=0 and fecha==caracas_now.date() and segundos_transcurridos<TIEMPO_MAX_ENCOLAMIENTO:
                            cedula=datosUsuario[0][0]
                            nombre=datosUsuario[0][1]
                            permisoAperturaInternet = datosUsuario[0][2]
                            permisoAperturaWifi = datosUsuario[0][3]
                            rol=datosUsuario[0][4]
                            usuario_id=datosUsuario[0][5]
                            cursor.execute('SELECT id, fecha_entrada, fecha_salida, entrada, salida, dia FROM web_horariospermitidos where usuario=%s', (usuario_id,))
                            horarios_permitidos = cursor.fetchall()
                            if horarios_permitidos != [] and permisoAperturaInternet == True and peticion_internet==True and rol=='Secundario':
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
                                        aperturaConcedidaInternet(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)   
                                        etapadiaapertura=1
                                    elif dia==diahoy and cantidaddias==1:
                                        hora=str(caracas_now)[11:19]
                                        horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                        fecha=str(caracas_now)[:10]
                                        etapadia=1
                                        if entrada<salida:
                                            if horahoy >= entrada and horahoy <= salida:
                                                #print('entrada concedida')
                                                aperturaConcedidaInternet(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)  
                                                etapadiaapertura=1
                                            else:
                                                aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                #print('fuera de horario')
                                        if entrada>salida:
                                            if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                                #print('entrada concedida')
                                                aperturaConcedidaInternet(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)   
                                                etapadiaapertura=1
                                            else:
                                                aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                #print('fuera de horario')
                                    elif dia==diahoy and cantidaddias>1:
                                        hora=str(caracas_now)[11:19]
                                        horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                        fecha=str(caracas_now)[:10]
                                        etapadia=1
                                        if entrada<salida:
                                            if horahoy >= entrada and horahoy <= salida:
                                                #print('entrada concedida')
                                                aperturaConcedidaInternet(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo) 
                                                etapadiaapertura=1
                                                contadoraux=0
                                            else:
                                                contadoraux = contadoraux+1
                                                if contadoraux == cantidaddias:
                                                    aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                    contadoraux=0
                                        if entrada>salida:
                                            if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                                #print('entrada concedida')
                                                aperturaConcedidaInternet(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo) 
                                                etapadiaapertura=1
                                                contadoraux=0
                                            else:
                                                contadoraux = contadoraux+1
                                                if contadoraux == cantidaddias:
                                                    aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                    contadoraux=0
                                                #print('fuera de horario')
                                if etapadia==0 and etapadiaapertura==0:
                                    aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                    #print('Dia no permitido')
                            elif rol=='Propietario' and permisoAperturaInternet == True and peticion_internet==True:
                                tz = pytz.timezone('America/Caracas')
                                caracas_now = datetime.now(tz)
                                hora=str(caracas_now)[11:19]
                                horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                fecha=str(caracas_now)[:10]
                                etapadia=1
                                aperturaConcedidaInternet(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)   
                            elif horarios_permitidos != [] and permisoAperturaInternet == True and peticion_internet==True and rol=='Visitante':
                                tz = pytz.timezone('America/Caracas')
                                caracas_now = datetime.now(tz)
                                horahoy = caracas_now.time()
                                fechahoy = caracas_now.date()
                                for horario_id, fecha_entrada, fecha_salida, entrada, salida, _ in horarios_permitidos:
                                    if (fechahoy==fecha_entrada and horahoy>=entrada) or (fechahoy > fecha_entrada and fechahoy<fecha_salida) or (fechahoy==fecha_salida and horahoy<=salida):
                                        permitir, aperturasRealizadas = controlhorariovisitante(cursor, conn, horario_id, razonApertura)
                                        if permitir:
                                            fecha=str(caracas_now)[:10]
                                            aperturaConcedidaInternetVisitante(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, horario_id, aperturasRealizadas, abriendo)
                                        else:
                                            aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud) 
                                    else:
                                        aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                            elif horarios_permitidos != [] and permisoAperturaWifi == True and peticion_internet == False and rol=='Secundario':
                                tz = pytz.timezone('America/Caracas')
                                caracas_now = datetime.now(tz)
                                dia = caracas_now.weekday()
                                diahoy = dias_semana[dia]
                                for _, _, _, entrada, salida, dia in horarios_permitidos:
                                    diasusuario.append(dia)
                                cantidaddias = diasusuario.count(dia)
                                for _, _, _, entrada, salida, dia in horarios_permitidos:
                                    if 'Siempre' in diasusuario or rol=='Propietario':
                                        hora=str(caracas_now)[11:19]
                                        horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                        fecha=str(caracas_now)[:10]
                                        etapadia=1
                                        aperturaConcedidaWifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)
                                        etapadiaapertura=1
                                    elif dia==diahoy and cantidaddias==1:
                                        hora=str(caracas_now)[11:19]
                                        horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                        fecha=str(caracas_now)[:10]
                                        etapadia=1
                                        if entrada<salida:
                                            if horahoy >= entrada and horahoy <= salida:
                                                #print('entrada concedida')
                                                aperturaConcedidaWifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)
                                                etapadiaapertura=1
                                            else:
                                                aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                #print('fuera de horario')
                                        if entrada>salida:
                                            if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                                #print('entrada concedida')
                                                aperturaConcedidaWifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)
                                                etapadiaapertura=1
                                            else:
                                                aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                #print('fuera de horario')
                                    elif dia==diahoy and cantidaddias>1:
                                        hora=str(caracas_now)[11:19]
                                        horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                        fecha=str(caracas_now)[:10]
                                        etapadia=1
                                        if entrada<salida:
                                            if horahoy >= entrada and horahoy <= salida:
                                                #print('entrada concedida')
                                                aperturaConcedidaWifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)
                                                etapadiaapertura=1
                                                contadoraux=0
                                            else:
                                                contadoraux = contadoraux+1
                                                if contadoraux == cantidaddias:
                                                    aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                    contadoraux=0
                                        if entrada>salida:
                                            if (horahoy>=entrada and horahoy <=ultimahora) or (horahoy>=primerahora and horahoy <= salida):
                                                #print('entrada concedida')
                                                aperturaConcedidaWifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)
                                                etapadiaapertura=1
                                                contadoraux=0
                                            else:
                                                contadoraux = contadoraux+1
                                                if contadoraux == cantidaddias:
                                                    aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                                    contadoraux=0
                                                #print('fuera de horario')
                                if etapadia==0 and etapadiaapertura==0:
                                    aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                                    #print('Dia no permitido')
                            elif rol=='Propietario' and permisoAperturaWifi == True and peticion_internet == False:
                                tz = pytz.timezone('America/Caracas')
                                caracas_now = datetime.now(tz)
                                hora=str(caracas_now)[11:19]
                                horahoy = datetime.strptime(hora, '%H:%M:%S').time()
                                fecha=str(caracas_now)[:10]
                                etapadia=1
                                aperturaConcedidaWifi(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, abriendo)
                            elif horarios_permitidos != [] and permisoAperturaWifi == True and peticion_internet == False and rol=='Visitante':
                                tz = pytz.timezone('America/Caracas')
                                caracas_now = datetime.now(tz)
                                horahoy = caracas_now.time()
                                fechahoy = caracas_now.date()
                                for horario_id, fecha_entrada, fecha_salida, entrada, salida, _ in horarios_permitidos:
                                    if (fechahoy==fecha_entrada and horahoy>=entrada) or (fechahoy > fecha_entrada and fechahoy<fecha_salida) or (fechahoy==fecha_salida and horahoy<=salida):
                                        permitir, aperturasRealizadas = controlhorariovisitante(cursor, conn, horario_id, razonApertura)
                                        if permitir:
                                            fecha=str(caracas_now)[:10]
                                            aperturaConcedidaWifiVisitante(nombre, fecha, horahoy, CONTRATO, cedula, cursor, conn, acceso_solicitud, id_solicitud, razonApertura, horario_id, aperturasRealizadas, abriendo)
                                        else:
                                            aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud) 
                                    else:
                                        aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud)
                            else:
                                aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud) 
                                #print('este usuario no tiene horarios establecidos')
                            diasusuario=[]
                        else:
                            aperturadenegada(cursor, conn, acceso_solicitud, id_solicitud) 
    except (Exception, psycopg2.Error) as error:
        print(f"{error} - fallo en hacer las consultas en base de datos de aperturas")
        total=0
    finally:
        print("se ha cerrado la conexion a la base de datos")
        if conn:
            cursor.close()
            conn.close()
            total=0
