import psycopg2
import os
import time
connlocal = None
cursorlocal=None
total=0

SERVIDOR_LOCAL=os.environ.get('URL_SERVIDOR')

######################################
#############ACCESOS###################
#######################################
acceso1=os.environ.get('URL_ACCESO1')
acceso2=os.environ.get('URL_ACCESO2')
acceso3=os.environ.get('URL_ACCESO3')
acceso4=os.environ.get('URL_ACCESO4')

descripcion_acceso1=os.environ.get('RAZON_ACCESO1')
descripcion_acceso2=os.environ.get('RAZON_ACCESO2')
descripcion_acceso3=os.environ.get('RAZON_ACCESO3')
descripcion_acceso4=os.environ.get('RAZON_ACCESO4')

######################################
#############CAPTAHUELLAS#############
#######################################

captahuella1=os.environ.get('URL_CAPTAHUELLA1')
captahuella2=os.environ.get('URL_CAPTAHUELLA2')
captahuella3=os.environ.get('URL_CAPTAHUELLA3')
captahuella4=os.environ.get('URL_CAPTAHUELLA4')
# captahuella5=os.environ.get('URL_CAPTAHUELLA5')
# captahuella6=os.environ.get('URL_CAPTAHUELLA6')
# captahuella7=os.environ.get('URL_CAPTAHUELLA7')
# captahuella8=os.environ.get('URL_CAPTAHUELLA8')

descripcion_captahuella1=os.environ.get('RAZON_CAPTAHUELLA1')
descripcion_captahuella2=os.environ.get('RAZON_CAPTAHUELLA2')
descripcion_captahuella3=os.environ.get('RAZON_CAPTAHUELLA3')
descripcion_captahuella4=os.environ.get('RAZON_CAPTAHUELLA4')
# descripcion_captahuella5=os.environ.get('RAZON_CAPTAHUELLA5')
# descripcion_captahuella6=os.environ.get('RAZON_CAPTAHUELLA6')
# descripcion_captahuella7=os.environ.get('RAZON_CAPTAHUELLA7')
# descripcion_captahuella8=os.environ.get('RAZON_CAPTAHUELLA8')

######################################
################RFID###################
#######################################

rfid1=os.environ.get('URL_RFID1')
rfid2=os.environ.get('URL_RFID2')
rfid3=os.environ.get('URL_RFID3')
rfid4=os.environ.get('URL_RFID4')
# rfid5=os.environ.get('URL_RFID5')
# rfid6=os.environ.get('URL_RFID6')
# rfid7=os.environ.get('URL_RFID7')
# rfid8=os.environ.get('URL_RFID8')

descripcion_rfid1=os.environ.get('RAZON_RFID1')
descripcion_rfid2=os.environ.get('RAZON_RFID2')
descripcion_rfid3=os.environ.get('RAZON_RFID3')
descripcion_rfid4=os.environ.get('RAZON_RFID4')
# descripcion_rfid5=os.environ.get('RAZON_RFID5')
# descripcion_rfid6=os.environ.get('RAZON_RFID6')
# descripcion_rfid7=os.environ.get('RAZON_RFID7')
# descripcion_rfid8=os.environ.get('RAZON_RFID8')


dispositivos=[acceso1, acceso2, acceso3, acceso4,
              captahuella1, captahuella2, captahuella3, captahuella4, 
              rfid1, rfid2, rfid3, rfid4,
              SERVIDOR_LOCAL
             ]

dispositivos_dict ={acceso1:descripcion_acceso1, 
                    acceso2:descripcion_acceso2, 
                    acceso3:descripcion_acceso3, 
                    acceso4:descripcion_acceso4, 
                    captahuella1:descripcion_captahuella1, 
                    captahuella2:descripcion_captahuella2, 
                    captahuella3:descripcion_captahuella3, 
                    captahuella4:descripcion_captahuella4, 
                    rfid1:descripcion_rfid1, 
                    rfid2:descripcion_rfid2,
                    rfid3:descripcion_rfid3,
                    rfid4:descripcion_rfid4,
                    SERVIDOR_LOCAL:'SERVIDOR LOCAL'
                    }

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


        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_usuarios (cedula varchar(150), nombre varchar(150), telegram_id varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_interacciones (nombre varchar(150), fecha date, hora time without time zone, razon varchar(150), contrato varchar(150), cedula_id varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_horariospermitidos (entrada time without time zone, salida time without time zone, cedula_id varchar(150), dia varchar(180))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS dias_acumulados (fecha varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_dispositivos (dispositivo varchar(150), descripcion varchar(150), estado varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_huellas (id_suprema integer, cedula varchar(150), template text)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_tagsrfid (epc text, cedula varchar(150))')
        #cursorlocal.execute('CREATE TABLE IF NOT EXISTS led (onoff integer, acceso integer)')
        connlocal.commit()

        cursorlocal.execute('SELECT*FROM web_dispositivos')
        tabladispositivos= cursorlocal.fetchall()

        if len(tabladispositivos) < 1:
            for dispositivo in dispositivos:
                if dispositivo:
                    descripcion = dispositivos_dict[dispositivo]
                    if dispositivo == SERVIDOR_LOCAL:
                        estado = '1'
                    else:
                        estado = '0'
                    cursorlocal.execute('INSERT INTO web_dispositivos values(%s, %s, %s)',(dispositivo, descripcion, estado))
                    connlocal.commit()
        # cursorlocal.execute('SELECT*FROM led')
        # tablaled= cursorlocal.fetchall()
        # if len(tablaled) < 1:
        #     cursorlocal.execute('INSERT INTO led values(0,1)')
        #     connlocal.commit()
        #     cursorlocal.execute('INSERT INTO led values(0,2)')
        #     connlocal.commit()
        #     cursorlocal.execute('INSERT INTO led values(0,3)')
        #     connlocal.commit()
        #     cursorlocal.execute('INSERT INTO led values(0,4)')
        #     connlocal.commit()

    except (Exception, psycopg2.Error) as error:
        print("fallo en hacer las consultas")
        if connlocal:
            cursorlocal.close()
            connlocal.close()

    finally:
        print("se ha cerrado la conexion a la base de datos")
        if connlocal:
            cursorlocal.close()
            connlocal.close()
        break
    
