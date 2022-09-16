import psycopg2
import os
import subprocess
import time
import pytz
from datetime import datetime

CONTRATO=os.environ.get("CONTRATO")
maximo_dias_acumular=int(os.environ.get("DIAS_ACUMULAR"))
connlocal = None
connheroku = None
cursorheroku=None
cursorlocal=None
listausuariosheroku=[]
listausuarioslocal=[]
total=0
etapa=0
fechahoy=None
fechaayer=None
diasacumulados=[]

while True:
    
    t1=time.perf_counter()
    while total<=5:
        t2=time.perf_counter()
        total=t2-t1
    total=0
    try:
        
        #con esto se apunta a la base de datos local
        connlocal = psycopg2.connect(
            database=os.environ.get("DATABASE"), 
            user=os.environ.get("USERDB"), 
            password=os.environ.get("PASSWORD"), 
            host=os.environ.get("HOST"), 
            port=os.environ.get("PORT")
        )
        cursorlocal = connlocal.cursor()
        
        conn_info = subprocess.run(["heroku", "config:get", "DATABASE_URL", "-a", 'tesis-reconocimiento-facial'], stdout = subprocess.PIPE)
        connuri = conn_info.stdout.decode('utf-8').strip()
        connheroku = psycopg2.connect(connuri)
        cursorheroku = connheroku.cursor()
        
        while True:


            if etapa==0:
                tz = pytz.timezone('America/Caracas')
                caracas_now = datetime.now(tz)
                fechahoy=str(caracas_now)[:10]

                if fechahoy != fechaayer:
                    fechaayer=fechahoy
                    tupla_fecha_hoy=(fechahoy,)
                    cursorlocal.execute('SELECT fecha FROM dias_acumulados')
                    dias_acumulados= cursorlocal.fetchall()
                    nro_dias_acumulados=len(dias_acumulados)

                    if nro_dias_acumulados >= maximo_dias_acumular:
                        cursorlocal.execute('DELETE FROM web_interacciones *')
                        cursorlocal.execute('DELETE FROM dias_acumulados *')
                        connlocal.commit()
                        
                    if not tupla_fecha_hoy in dias_acumulados:
                        cursorlocal.execute('''INSERT INTO dias_acumulados (fecha)
                        VALUES (%s);''', (fechahoy,))
                        connlocal.commit()

                cursorlocal.execute('SELECT * FROM web_interacciones where contrato=%s and fecha=%s', (CONTRATO,fechahoy))
                interacciones_local= cursorlocal.fetchall()
                cursorheroku.execute('SELECT nombre, fecha, hora, razon, contrato, cedula FROM web_interacciones where contrato=%s and fecha=%s', (CONTRATO,fechahoy))
                interacciones_heroku= cursorheroku.fetchall()

                nro_int_local = len(interacciones_local)
                nro_int_heroku = len(interacciones_heroku)

                if nro_int_local != nro_int_heroku:

                    for interaccion in interacciones_local:
                        try:
                            interacciones_heroku.index(interaccion)
                        except ValueError:
                            nombre=interaccion[0]
                            fecha=interaccion[1]
                            hora=interaccion[2]
                            razon=interaccion[3]
                            cedula=interaccion[5]
                            cursorheroku.execute('''INSERT INTO web_interacciones (nombre, fecha, hora, razon, contrato, cedula)
                            VALUES (%s, %s, %s, %s, %s, %s);''', (nombre, fecha, hora, razon, CONTRATO, cedula))
                            connheroku.commit()
                    
                    nombre=None
                    fecha=None
                    hora=None
                    razon=None
                    cedula=None
                etapa=1

            if etapa==1:

                cursorlocal.execute('SELECT * FROM web_usuarios where contrato_id=%s', (CONTRATO,))
                usuarios_local= cursorlocal.fetchall()

                cursorheroku.execute('SELECT cedula, nombre, telegram_id, contrato_id FROM web_usuarios where contrato_id=%s', (CONTRATO,))
                usuarios_heroku= cursorheroku.fetchall()


                for usuario in usuarios_local:
                    cedula=usuario[0]
                    try:
                        listausuarioslocal.index(cedula)
                    except ValueError:
                        listausuarioslocal.append(cedula)
                
                for usuario in listausuarioslocal:
                    cursorheroku.execute('SELECT entrada, salida, cedula, dia FROM web_horariospermitidos WHERE cedula=%s and contrato_id=%s',(usuario,CONTRATO))
                    diasheroku= cursorheroku.fetchall()
                    
                    cursorlocal.execute('SELECT * FROM web_horariospermitidos WHERE cedula_id=%s',(usuario,))
                    diaslocal= cursorlocal.fetchall()

                    if len(diasheroku) > 0 and len(diasheroku) > len(diaslocal):
                        for diasherokuiterar in diasheroku:
                            try:
                                diaslocal.index(diasherokuiterar)
                            except ValueError:
                                entrada=diasherokuiterar[0]
                                salida=diasherokuiterar[1]
                                cedula=diasherokuiterar[2]
                                dia=diasherokuiterar[3]
                                cursorlocal.execute('''INSERT INTO web_horariospermitidos (entrada, salida, cedula_id, dia)
                                VALUES (%s, %s, %s, %s);''', (entrada, salida, cedula, dia))
                                connlocal.commit()

                    if len(diaslocal) > len(diasheroku):
                        for diaslocaliterar in diaslocal:
                            try:
                                diasheroku.index(diaslocaliterar)
                            except ValueError:
                                entrada=diaslocaliterar[0]
                                salida=diaslocaliterar[1]
                                cedula=diaslocaliterar[2]
                                dia=diaslocaliterar[3]
                                cursorlocal.execute('DELETE FROM web_horariospermitidos WHERE entrada=%s AND salida=%s AND cedula_id=%s AND dia=%s',(entrada, salida, cedula, dia))
                                connlocal.commit()
                diaslocal=[]
                diasheroku=[]
                listausuariosheroku=[]
                listausuarioslocal=[]
                etapa=2

            if etapa==2:

                cursorlocal.execute('SELECT * FROM web_usuarios where contrato_id=%s', (CONTRATO,))
                usuarios_local= cursorlocal.fetchall()

                cursorheroku.execute('SELECT cedula, nombre, telegram_id, contrato_id FROM web_usuarios where contrato_id=%s', (CONTRATO,))
                usuarios_heroku= cursorheroku.fetchall()
                
                nro_usu_local = len(usuarios_local)
                nro_usu_heroku = len(usuarios_heroku)
            
                if nro_usu_heroku == nro_usu_local:
                    for usuario in usuarios_heroku:
                        try:
                            usuarios_local.index(usuario)
                        except ValueError:
                            cedula=usuario[0]
                            telegram_id=usuario[2]
                            cursorlocal.execute("UPDATE web_usuarios SET telegram_id=%s WHERE cedula=%s", (telegram_id,cedula))
                            connlocal.commit()
                etapa=3

            if etapa==3:

                cursorlocal.execute('SELECT * FROM web_usuarios where contrato_id=%s', (CONTRATO,))
                usuarios_local= cursorlocal.fetchall()

                cursorheroku.execute('SELECT cedula, nombre, telegram_id, contrato_id FROM web_usuarios where contrato_id=%s', (CONTRATO,))
                usuarios_heroku= cursorheroku.fetchall()

                nro_usu_local = len(usuarios_local)
                nro_usu_heroku = len(usuarios_heroku)
            
                #cuando se va a eliminar un usuario
                if nro_usu_local > nro_usu_heroku:

                    for usuario in usuarios_heroku:
                        cedula=usuario[0]
                        try:
                            listausuariosheroku.index(cedula)
                        except ValueError:
                            listausuariosheroku.append(cedula)
                    
                    for usuario in usuarios_local:
                        cedula=usuario[0]
                        try:
                            listausuarioslocal.index(cedula)
                        except ValueError:
                            listausuarioslocal.append(cedula)

                    for usuario in listausuarioslocal:
                        try:
                            listausuariosheroku.index(usuario)
                        except ValueError:
                            cursorlocal.execute('DELETE FROM web_usuarios WHERE cedula=%s', (usuario,))
                            cursorlocal.execute('DELETE FROM web_horariospermitidos WHERE cedula_id=%s', (usuario,))
                            connlocal.commit()
                    listausuariosheroku=[]
                    listausuarioslocal=[]

                # cuando se va a agregar usuarios
                if nro_usu_heroku > nro_usu_local:

                    for usuario in usuarios_heroku:
                        cedula=usuario[0]
                        try:
                            listausuariosheroku.index(cedula)
                        except ValueError:
                            listausuariosheroku.append(cedula)
                    
                    for usuario in usuarios_local:
                        cedula=usuario[0]
                        try:
                            listausuarioslocal.index(cedula)
                        except ValueError:
                            listausuarioslocal.append(cedula)

                    for usuario in listausuariosheroku:
                        try:
                            listausuarioslocal.index(usuario)
                        except ValueError:
                            cursorheroku.execute('SELECT cedula, nombre, telegram_id, contrato_id FROM web_usuarios where cedula=%s and contrato_id=%s', (usuario, CONTRATO))
                            usuario_heroku= cursorheroku.fetchall()
                            cedula=usuario_heroku[0][0]
                            nombre=usuario_heroku[0][1]
                            cursorlocal.execute('''INSERT INTO web_usuarios (cedula, nombre, contrato_id)
                            VALUES (%s, %s, %s)''', (cedula, nombre, CONTRATO))
                            connlocal.commit()
                    listausuariosheroku=[]
                    listausuarioslocal=[]
                etapa=4
            
            if etapa==4:
                cursorlocal.execute('SELECT * FROM web_dispositivos')
                dispositivos_local= cursorlocal.fetchall()

                cursorheroku.execute('SELECT dispositivo, descripcion, estado FROM web_dispositivos where contrato_id=%s', (CONTRATO,))
                dispositivos_heroku= cursorheroku.fetchall()

                if len(dispositivos_heroku) != len(dispositivos_local):
                    cursorheroku.execute('''DELETE FROM web_dispositivos * WHERE contrato_id=%s''', (CONTRATO,))
                    connheroku.commit()
                    cursorheroku.execute('SELECT dispositivo, descripcion, estado FROM web_dispositivos where contrato_id=%s', (CONTRATO,))
                    dispositivos_heroku= cursorheroku.fetchall()
                    for dispositivolocal in dispositivos_local:
                        try:
                            dispositivos_heroku.index(dispositivolocal)
                        except ValueError:
                            tz = pytz.timezone('America/Caracas')
                            caracas_now = datetime.now(tz)
                            fechaahora=str(caracas_now)[:10]
                            hora=str(caracas_now)[11:19]
                            horaahora = datetime.strptime(hora, '%H:%M:%S').time()
                            dispositivo=dispositivolocal[0]
                            descripcion=dispositivolocal[1]
                            estado=dispositivolocal[2]
                            cursorheroku.execute('''INSERT INTO web_dispositivos (dispositivo, descripcion, estado, contrato_id, fecha, hora)
                            VALUES (%s, %s, %s, %s, %s, %s);''', (dispositivo, descripcion, estado, CONTRATO, fechaahora, horaahora))
                            connheroku.commit()
                else:
                    for dispositivolocal in dispositivos_local:
                        try:
                            dispositivos_heroku.index(dispositivolocal)
                        except ValueError:
                            tz = pytz.timezone('America/Caracas')
                            caracas_now = datetime.now(tz)
                            fechaahora=str(caracas_now)[:10]
                            hora=str(caracas_now)[11:19]
                            horaahora = datetime.strptime(hora, '%H:%M:%S').time()
                            dispositivo=dispositivolocal[0]
                            descripcion=dispositivolocal[1]
                            estado=dispositivolocal[2]
                            cursorheroku.execute('UPDATE web_dispositivos SET estado=%s, fecha=%s, hora=%s WHERE dispositivo=%s AND descripcion=%s AND contrato_id=%s;', 
                            (estado,fechaahora,horaahora, dispositivo, descripcion, CONTRATO))
                            connlocal.commit()
                etapa=0

    except (Exception, psycopg2.Error) as error:
        print("fallo en hacer las consultas")
        if connlocal:
            cursorlocal.close()
            connlocal.close()
        if connheroku:
            cursorheroku.close()
            connheroku.close()
    finally:
        print("se ha cerrado la conexion a la base de datos")
        if connlocal:
            cursorlocal.close()
            connlocal.close()
        if connheroku:
            cursorheroku.close()
            connheroku.close()
