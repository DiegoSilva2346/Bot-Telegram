          
import datetime             
import operator       
import random           
import psycopg2        
"""El sistema de base de datos consiste en :      
-Tabla USUARIOS + NUMERO ID_CHAT , donde se almacena toda la informacion de cada chat grupal(numeros id_de los participanetes, nombres,comentarios, fecha del momento en que se realizo el  comentario)         
-Tabla CHAT_ID  donde se alamcena todos los numeros de  id_chat de todos los grupos donde esta el bot     
-Tabla USUARIOS_TEMP + NUMERO ID_CHAT , donde se almacena solo por 24hs toda la informacion de cada chat grupal, cada 24hs se borra la informacion de todas las tablas    """  
 
def select_decorator(punto=False,lista=False):#Funcion decoradora que realiza multiples consultas sql.
  def crud_decorator(function):          
   def conexion_database(*args, **kwargs):         
      
    conect=psycopg2.connect(database="name_database",user="postgres",password="your_password",host="localhost")     
    micursor=conect.cursor()            

            
        
    if punto==True:#Punto es una variable , que me permite diferenciar  las consultas de seleccion de las demas.           
     user_coment={}               
     diccionario={}           
     maximos=list()        
     sentencias_sql,mensaje=function(*args, **kwargs)           
         
     micursor.execute(sentencias_sql[0])       
     lista=micursor.fetchall()             
     if len(lista)==0:             
      mensaje="Nadie comento en las ultimas 24 hs" 
     elif len(lista)>0: 
      set_users=set(lista)         
            
      lista_users=list(set_users)        
             
      for usuario in lista_users:           
      
       diccionario[usuario[0]]=usuario[1]      

  
       micursor.execute(sentencias_sql[1]+str(usuario[1]))              
       lista2=micursor.fetchall()          
       user_coment[usuario[0]]=len(lista2)     
                
      clients_sort = sorted(user_coment.items(), key=operator.itemgetter(1), reverse=True)             
                  

      if "USUARIOS_TEMP" in sentencias_sql[1] :          
        maximo=clients_sort[0][1]      
        lista_ganadores=list()          
        for dic in clients_sort:     
          if dic[1]==maximo:       
            lista_ganadores.append(dic[0])           
        if len(lista_ganadores)==1:        
          mensaje=lista_ganadores[0]+" es el usuario que mas comento en las ultimas 24hs con " +str(maximo)+" comentarios"        
        elif len(lista_ganadores)>1:      
          mensaje=",".join(lista_ganadores)+" son los usuarios que mas comentaron en las ultimas 24hs con "+str(maximo)+" comentarios"            
      else:
       for name in enumerate(clients_sort):        
        mensaje=mensaje+" "+str(name[1][0]) +" "+ 'numero de comentarios'+" "+ str(user_coment[name[1][0]])+"\n"
 
          
                
       if len(lista)>3000:            
        try:      
          mensaje=mensaje+"\n Sobrepasamos el  comentario numero 3000 , eliminare toda la informacion de mi base de datos"             
             
          micursor.execute(sententencias_sql[2])            
               
        except:      
         mensaje=message+"\n  Sobrepasamos el comentario numero 3000 , pero no pude eliminar toda la informacion de mi base de datos , se produjo un error."         
       micursor.close() 
       conect.close()             
         
      return mensaje
    else:           
                              
     try:              
       sentencia_sql=function(*args,**kwargs)              
       micursor.execute(sentencia_sql)           
             

       conect.commit()               
       micursor.close() 
       conect.close()             
     except:            
       print("la tabla ya existe")
   return conexion_database             
  return crud_decorator
@select_decorator(punto=False)       
def create_table_chat_id():       
   try:          
     sentencia_sql="CREATE TABLE CHAT_ID (CHAT_ID VARCHAR(100) UNIQUE)  "              
  
   except:         
       print("La tabla CHAT_ID  ya existe, pasando a la siguiente ejecucion")               
   return sentencia_sql                  
@select_decorator(punto=False)                
def create_table_temp_chat_id(chat_id):         
   try:            
    chat_id=str(chat_id).replace("-","")             
    sentencia_sql="CREATE TABLE USUARIOS_TEMP"+chat_id+" (ID_USER INTEGER ,NOMBRE_USUARIO VARCHAR(60), COMENTARIO VARCHAR(2350),fecha date)"          
     
    print("Finalice create  table data chat id")      
   except:         
       print("La tabla create_table_temp_chat_id ya existe, pasando a la siguiente ejucion")          
   return sentencia_sql               
@select_decorator(punto=False)          
def append_user_temp(data_user,chat_id):       
   chat_id=str(chat_id).replace("-","")              
   sentencia_sql="INSERT INTO "+"USUARIOS_TEMP"+chat_id+ " VALUES"+str(data_user)                
   return sentencia_sql
@select_decorator(punto=False)           
def delete_user_temp(lista_chats_id):         
   id_chat=lista_chats_id        
   id_chat=str(lista_chats_id).replace("-","")  
   sentencia_sql="DELETE FROM USUARIOS_TEMP"+str(id_chat)        
   return sentencia_sql  

@select_decorator(punto=False)         
def create_table_data_chat(chat_id):              
   try:            
    chat_id=str(chat_id).replace("-","")           
    sentencia_sql="CREATE TABLE USUARIOS"+chat_id+" (ID_USER INTEGER ,NOMBRE_USUARIO VARCHAR(60), COMENTARIO VARCHAR(2350),fecha date)"     
   except:         
       pass           
   return sentencia_sql                 
@select_decorator(punto=False) 
def append_user(data_user,chat_id):              
   print(data_user)                
   chat_id=str(chat_id).replace("-","")          
   sentencia_sql="INSERT INTO "+"USUARIOS"+chat_id+ " VALUES"+str(data_user)      
   return sentencia_sql 
@select_decorator(punto=True)           
def select_user(chat_id):            
   chat_id=str(chat_id).replace("-","")               
   chat_id=str(chat_id)           
   sentencias_sql=["SELECT NOMBRE_USUARIO,ID_USER FROM USUARIOS"+str(chat_id),"SELECT * from USUARIOS"+str(chat_id) +" WHERE ID_USER =","DELETE FROM USUARIOS"+str(chat_id),]     
   mensaje=""
   return sentencias_sql,mensaje
@select_decorator(punto=False)        
def insert_chat_id(chat_id):                  
  try:     
   print("Estoy en insert chat id")               
   chat_id=str(chat_id).replace("-","")           
   sentencia_sql="""INSERT INTO CHAT_ID  VALUES ("""+str(chat_id)+""")"""          
 
  except:      
     pass 
  return sentencia_sql             
       
def select_chatid():          
   miconexion=psycopg2.connect(database="name_database",user="postgres",password="your_password",host="localhost")  
   micursor=miconexion.cursor()          
   micursor.execute("SELECT * FROM CHAT_ID");              
   lista=micursor.fetchall()           
   
   miconexion.commit()              
   micursor.close() 
   miconexion.close()           
   return lista
@select_decorator(punto=True)           
def select_user_temp(chat_id):            
   chat_id=str(chat_id).replace("-","")             
   sentencias_sql=["SELECT NOMBRE_USUARIO,ID_USER FROM USUARIOS_TEMP"+str(chat_id),"SELECT * from USUARIOS_TEMP"+str(chat_id) +" WHERE ID_USER ="]    
   mensaje=""       
   return sentencias_sql,mensaje
   
