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

descripcion_acceso1=os.environ.get('RAZON_ACCESO1')
descripcion_acceso2=os.environ.get('RAZON_ACCESO2')
descripcion_acceso3=os.environ.get('RAZON_ACCESO3')
descripcion_acceso4=os.environ.get('RAZON_ACCESO4')
descripcion_acceso5=os.environ.get('RAZON_ACCESO5')
descripcion_acceso6=os.environ.get('RAZON_ACCESO6')
descripcion_acceso7=os.environ.get('RAZON_ACCESO7')
descripcion_acceso8=os.environ.get('RAZON_ACCESO8')
descripcion_acceso9=os.environ.get('RAZON_ACCESO9')
descripcion_acceso10=os.environ.get('RAZON_ACCESO10')
descripcion_acceso11=os.environ.get('RAZON_ACCESO11')
descripcion_acceso12=os.environ.get('RAZON_ACCESO12')
descripcion_acceso13=os.environ.get('RAZON_ACCESO13')
descripcion_acceso14=os.environ.get('RAZON_ACCESO14')
descripcion_acceso15=os.environ.get('RAZON_ACCESO15')
descripcion_acceso16=os.environ.get('RAZON_ACCESO16')
descripcion_acceso17=os.environ.get('RAZON_ACCESO17')
descripcion_acceso18=os.environ.get('RAZON_ACCESO18')
descripcion_acceso19=os.environ.get('RAZON_ACCESO19')
descripcion_acceso20=os.environ.get('RAZON_ACCESO20')

######################################
#############CAPTAHUELLAS#############
#######################################

captahuella1=os.environ.get('URL_CAPTAHUELLA1')
captahuella2=os.environ.get('URL_CAPTAHUELLA2')
captahuella3=os.environ.get('URL_CAPTAHUELLA3')
captahuella4=os.environ.get('URL_CAPTAHUELLA4')
captahuella5=os.environ.get('URL_CAPTAHUELLA5')
captahuella6=os.environ.get('URL_CAPTAHUELLA6')
captahuella7=os.environ.get('URL_CAPTAHUELLA7')
captahuella8=os.environ.get('URL_CAPTAHUELLA8')
captahuella9=os.environ.get('URL_CAPTAHUELLA9')
captahuella10=os.environ.get('URL_CAPTAHUELLA10')
captahuella11=os.environ.get('URL_CAPTAHUELLA11')
captahuella12=os.environ.get('URL_CAPTAHUELLA12')
captahuella13=os.environ.get('URL_CAPTAHUELLA13')
captahuella14=os.environ.get('URL_CAPTAHUELLA14')
captahuella15=os.environ.get('URL_CAPTAHUELLA15')
captahuella16=os.environ.get('URL_CAPTAHUELLA16')
captahuella17=os.environ.get('URL_CAPTAHUELLA17')
captahuella18=os.environ.get('URL_CAPTAHUELLA18')
captahuella19=os.environ.get('URL_CAPTAHUELLA19')
captahuella20=os.environ.get('URL_CAPTAHUELLA20')

descripcion_captahuella1=os.environ.get('RAZON_CAPTAHUELLA1')
descripcion_captahuella2=os.environ.get('RAZON_CAPTAHUELLA2')
descripcion_captahuella3=os.environ.get('RAZON_CAPTAHUELLA3')
descripcion_captahuella4=os.environ.get('RAZON_CAPTAHUELLA4')
descripcion_captahuella5=os.environ.get('RAZON_CAPTAHUELLA5')
descripcion_captahuella6=os.environ.get('RAZON_CAPTAHUELLA6')
descripcion_captahuella7=os.environ.get('RAZON_CAPTAHUELLA7')
descripcion_captahuella8=os.environ.get('RAZON_CAPTAHUELLA8')
descripcion_captahuella9=os.environ.get('RAZON_CAPTAHUELLA9')
descripcion_captahuella10=os.environ.get('RAZON_CAPTAHUELLA10')
descripcion_captahuella11=os.environ.get('RAZON_CAPTAHUELLA11')
descripcion_captahuella12=os.environ.get('RAZON_CAPTAHUELLA12')
descripcion_captahuella13=os.environ.get('RAZON_CAPTAHUELLA13')
descripcion_captahuella14=os.environ.get('RAZON_CAPTAHUELLA14')
descripcion_captahuella15=os.environ.get('RAZON_CAPTAHUELLA15')
descripcion_captahuella16=os.environ.get('RAZON_CAPTAHUELLA16')
descripcion_captahuella17=os.environ.get('RAZON_CAPTAHUELLA17')
descripcion_captahuella18=os.environ.get('RAZON_CAPTAHUELLA18')
descripcion_captahuella19=os.environ.get('RAZON_CAPTAHUELLA19')
descripcion_captahuella20=os.environ.get('RAZON_CAPTAHUELLA20')

######################################
################RFID###################
#######################################

rfid1=os.environ.get('URL_RFID1')
rfid2=os.environ.get('URL_RFID2')
rfid3=os.environ.get('URL_RFID3')
rfid4=os.environ.get('URL_RFID4')
rfid5=os.environ.get('URL_RFID5')
rfid6=os.environ.get('URL_RFID6')
rfid7=os.environ.get('URL_RFID7')
rfid8=os.environ.get('URL_RFID8')
rfid9=os.environ.get('URL_RFID9')
rfid10=os.environ.get('URL_RFID10')
rfid11=os.environ.get('URL_RFID11')
rfid12=os.environ.get('URL_RFID12')
rfid13=os.environ.get('URL_RFID13')
rfid14=os.environ.get('URL_RFID14')
rfid15=os.environ.get('URL_RFID15')
rfid16=os.environ.get('URL_RFID16')
rfid17=os.environ.get('URL_RFID17')
rfid18=os.environ.get('URL_RFID18')
rfid19=os.environ.get('URL_RFID19')
rfid20=os.environ.get('URL_RFID20')

descripcion_rfid1=os.environ.get('RAZON_RFID1')
descripcion_rfid2=os.environ.get('RAZON_RFID2')
descripcion_rfid3=os.environ.get('RAZON_RFID3')
descripcion_rfid4=os.environ.get('RAZON_RFID4')
descripcion_rfid5=os.environ.get('RAZON_RFID5')
descripcion_rfid6=os.environ.get('RAZON_RFID6')
descripcion_rfid7=os.environ.get('RAZON_RFID7')
descripcion_rfid8=os.environ.get('RAZON_RFID8')
descripcion_rfid9=os.environ.get('RAZON_RFID9')
descripcion_rfid10=os.environ.get('RAZON_RFID10')
descripcion_rfid11=os.environ.get('RAZON_RFID11')
descripcion_rfid12=os.environ.get('RAZON_RFID12')
descripcion_rfid13=os.environ.get('RAZON_RFID13')
descripcion_rfid14=os.environ.get('RAZON_RFID14')
descripcion_rfid15=os.environ.get('RAZON_RFID15')
descripcion_rfid16=os.environ.get('RAZON_RFID16')
descripcion_rfid17=os.environ.get('RAZON_RFID17')
descripcion_rfid18=os.environ.get('RAZON_RFID18')
descripcion_rfid19=os.environ.get('RAZON_RFID19')
descripcion_rfid20=os.environ.get('RAZON_RFID20')

dispositivos=[captahuella1, captahuella2, captahuella3, captahuella4, captahuella5,
              captahuella6, captahuella7, captahuella8, captahuella9, captahuella10,
              captahuella11, captahuella12, captahuella3, captahuella14, captahuella15,
              captahuella16, captahuella17, captahuella18, captahuella19, captahuella20,
              rfid1, rfid2, rfid3, rfid4, rfid5,
              rfid6, rfid7, rfid8, rfid9, rfid10,
              rfid11, rfid12, rfid13, rfid14, rfid15,
              rfid16, rfid17, rfid18, rfid19, rfid20,
              SERVIDOR_LOCAL
             ]

dispositivos_dict= {captahuella1:descripcion_captahuella1, 
                    captahuella2:descripcion_captahuella2, 
                    captahuella3:descripcion_captahuella3, 
                    captahuella4:descripcion_captahuella4, 
                    captahuella5:descripcion_captahuella5,
                    captahuella6:descripcion_captahuella6, 
                    captahuella7:descripcion_captahuella7, 
                    captahuella8:descripcion_captahuella8, 
                    captahuella9:descripcion_captahuella9, 
                    captahuella10:descripcion_captahuella10,
                    captahuella11:descripcion_captahuella11, 
                    captahuella12:descripcion_captahuella12, 
                    captahuella13:descripcion_captahuella13, 
                    captahuella14:descripcion_captahuella14, 
                    captahuella15:descripcion_captahuella15,
                    captahuella16:descripcion_captahuella16, 
                    captahuella17:descripcion_captahuella17, 
                    captahuella18:descripcion_captahuella18, 
                    captahuella19:descripcion_captahuella19, 
                    captahuella20:descripcion_captahuella20,
                    rfid1:descripcion_rfid1, 
                    rfid2:descripcion_rfid2,
                    rfid3:descripcion_rfid3,
                    rfid4:descripcion_rfid4,
                    rfid5:descripcion_rfid5,
                    rfid6:descripcion_rfid6, 
                    rfid7:descripcion_rfid7,
                    rfid8:descripcion_rfid8,
                    rfid9:descripcion_rfid9,
                    rfid10:descripcion_rfid10,
                    rfid11:descripcion_rfid11, 
                    rfid12:descripcion_rfid12,
                    rfid13:descripcion_rfid13,
                    rfid14:descripcion_rfid14,
                    rfid15:descripcion_rfid15,
                    rfid16:descripcion_rfid16, 
                    rfid17:descripcion_rfid17,
                    rfid18:descripcion_rfid18,
                    rfid19:descripcion_rfid19,
                    rfid20:descripcion_rfid20,
                    SERVIDOR_LOCAL:'SERVIDOR LOCAL'
                    }

accesos_dispositivos=[acceso1, acceso2, acceso3, acceso4,
                      acceso5, acceso6, acceso7, acceso8,
                      acceso9, acceso10, acceso11, acceso12,
                      acceso13, acceso14, acceso15, acceso16,
                      acceso17, acceso18, acceso19, acceso20]

accesos_dispositivos_dict ={acceso1:descripcion_acceso1, 
                            acceso2:descripcion_acceso2, 
                            acceso3:descripcion_acceso3, 
                            acceso4:descripcion_acceso4,
                            acceso5:descripcion_acceso5, 
                            acceso6:descripcion_acceso6, 
                            acceso7:descripcion_acceso7, 
                            acceso8:descripcion_acceso8,
                            acceso9:descripcion_acceso9, 
                            acceso10:descripcion_acceso10, 
                            acceso11:descripcion_acceso11, 
                            acceso12:descripcion_acceso12,
                            acceso13:descripcion_acceso13, 
                            acceso14:descripcion_acceso14, 
                            acceso15:descripcion_acceso15, 
                            acceso16:descripcion_acceso16,
                            acceso17:descripcion_acceso17, 
                            acceso18:descripcion_acceso18, 
                            acceso19:descripcion_acceso19, 
                            acceso20:descripcion_acceso20
                            }

accesos_dict = {acceso1:"1", 
                acceso2:"2", 
                acceso3:"3", 
                acceso4:"4",
                acceso5:"5", 
                acceso6:"6", 
                acceso7:"7", 
                acceso8:"8",
                acceso9:"9", 
                acceso10:"10", 
                acceso11:"11", 
                acceso12:"12",
                acceso13:"13", 
                acceso14:"14", 
                acceso15:"15", 
                acceso16:"16",
                acceso17:"17", 
                acceso18:"18", 
                acceso19:"19", 
                acceso20:"20"
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

        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_usuarios (id integer, rol varchar, cedula varchar(150), nombre varchar(255), telegram_id varchar(150), numero_telefonico varchar(150), cedula_propietario varchar(150), unidad_id integer, entrada_beacon_uuid varchar(150), salida_beacon_uuid varchar(150), internet boolean, wifi boolean, bluetooth boolean, captahuella boolean, rfid boolean, facial boolean)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_interacciones (nombre varchar(255), fecha date, hora time without time zone, razon varchar(150), contrato text, cedula_id varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_logs_vigilantes (vigilante_id integer, vigilante_nombre varchar(150), unidad_id integer, unidad_nombre varchar(150), fecha date, hora time without time zone, razon varchar(150), contrato text, personas integer)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_logs_visitantes (vigilante_id integer, vigilante_nombre varchar(150), nombre varchar(255), fecha date, hora time without time zone, razon varchar(150), contrato text, cedula_id varchar(150), acompanantes integer, cedula_propietario varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_horariospermitidos (id integer, usuario integer, fecha_entrada date, fecha_salida date, entrada time without time zone, salida time without time zone, cedula_id varchar(150), dia varchar(180), acompanantes integer)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS dias_acumulados (fecha varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_dispositivos (dispositivo varchar(150), descripcion varchar(150), estado varchar(150), acceso varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_huellas (id_suprema integer, cedula varchar(150), template text)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_tagsrfid (epc text, cedula varchar(150))')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS solicitud_aperturas (id integer, id_usuario varchar(150), acceso varchar(150), razon varchar(150), estado integer, peticionInternet boolean, feedback boolean)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS accesos_abiertos (cedula varchar(150), acceso varchar(150), fecha date, hora time without time zone, estado boolean)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS control_horarios_visitantes (horario_id integer, aperturas_hechas integer)')
        cursorlocal.execute('CREATE TABLE IF NOT EXISTS web_unidades (id integer, nombre varchar(250), codigo integer)')
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
                    cursorlocal.execute('INSERT INTO web_dispositivos values(%s, %s, %s, %s)',(dispositivo, descripcion, estado, ""))
                    connlocal.commit()
            
            for dispositivoAcceso in accesos_dispositivos:
                if dispositivoAcceso:
                    descripcion = accesos_dispositivos_dict[dispositivoAcceso]
                    acceso = accesos_dict[dispositivoAcceso]
                    estado = '0'
                    cursorlocal.execute('INSERT INTO web_dispositivos values(%s, %s, %s, %s)',(dispositivoAcceso, descripcion, estado, acceso))
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
    
