import logging              
import telegram
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import database_users          
import datetime         
import random       
import os
# Enable logging          
PORT = int(os.environ.get('PORT', 5000))
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

TOKEN=os.getenv("TOKEN")         
mode=os.getenv("MODE")
        
if mode=="dev":    
  #Acceso local(desarrollo)        
  def run(updater):  
    updater.start_polling()     
    print("BOT CARGADO... ")     
    updater.idle()           
elif mode=="prod":         
  #Acceso HEROKU (produccion)   
  def run(updater):          
    PORT = int(os.environ.get('PORT', 5000))     
    HEROKU_APP_NAME=os.environ.get("HEROKU_APP_NAME")        
    updater.start_webhook(listen="0.0.0.0",
                          port=int(PORT),
                          url_path=TOKEN)
    updater.bot.setWebhook(f'https://{HEROKU_APP_NAME}.herokuapp.com/' + TOKEN)          
else:       
  logger.info("No se especifo el modo")        
  sys.exit()   





def time(update, context):       
    import datetime   
    try:      
        
      context.bot.send_message(chat_id=str(update.effective_chat["id"]), 
                              text="La hora de mi servidor: \n"+str(datetime.datetime.now())) 
    except:      
         
      context.bot.send_message(chat_id=str(update.effective_user["id"]), 
                              text="La hora de mi servidor: \n"+str(datetime.datetime.now()))               

def callback_minute(context: telegram.ext.CallbackContext):#Funcion que diariamente lanza avisos a los chats grupales donde esta el bot.            
         try:       
             #esto funciona para chat grupales      
                tupla_id=tuple(database_users.select_chatid())#Consigo todos los numeros de id(que representan de manera univoca a un chat grupal) de todos los chat donde esta el bot.          
                         
                for id_chat_tuple in tupla_id:            
                         
                 try:#El try impide caer el bot por si un chat grupal es elliminado por un usuario.          
                  
                  mensaje=database_users.select_user_temp(id_chat_tuple[0])            
                         
                  mensaje_global=database_users.select_user(id_chat_tuple[0])           
                               
                  context.bot.send_message(chat_id="-"+str(id_chat_tuple[0]), 
                              text=mensaje)             
                  context.bot.send_message(chat_id="-"+str(id_chat_tuple[0]), 
                              text="A continuaciòn la tabla acumulativa de comentarios : \n"+mensaje_global)              
                                        
                  database_users.delete_user_temp(id_chat_tuple[0])                               
                 except:        
                   pass                                      

                
                      
                     
                
                            
         except :           
                        
               pass   
def recept_message(update, context):
    
             
        context=context        
                 
        chat_id=update.effective_chat['id']       
        data_user=(update.effective_user["id"],update.effective_user['first_name'],str(update.message.text) ,str(datetime.datetime.now()) )            
                      
        if chat_id<0:#LOS NUMEROS DE CHAT GRUPAL EN TELEGRAM SON SIEMPRE NEGATIVOS          
         database_users.create_table_chat_id()
         database_users.create_table_temp_chat_id(chat_id)             
         database_users.create_table_data_chat(chat_id)          
         database_users.append_user_temp(data_user,chat_id)          
         
         database_users.insert_chat_id(chat_id)           
       
         database_users.append_user(data_user,chat_id)      


def help(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text('Help!')
def random(update,context) :         
    import random          
    try:      
      numero=random.randint(1,10)   
      context.bot.send_message(chat_id=str(update.effective_chat["id"]), 
                              text="El numero al azar entre 1 y 10 es: \n"+str(numero)) 
    except:      
      numero=random.randint(1,10)   
      context.bot.send_message(chat_id=str(update.effective_user["id"]), 
                              text="El numero al azar entre 1 y 10 es: \n"+str(numero))      

def echo(update, context):
    """Echo the user message."""
    update.message.reply_text(update.message.text)

def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)
def info(update,context):       
    try:         
          context.bot.send_message(chat_id=str(update.effective_chat["id"]), text="Silvabot1.0, puede hacer:\n1)Informar todos los dias cual es el usuario que mas comento durante las 24 hs y el numero de comentarios que realizo, si es que comento.\n2)Puedo alamacenar toda la informacion de tipo texto del chat grupal en mi base de datos Sqlite3.0,esto con la finalidad de en un futuro poder aprender a dialogar con ustedes.\n3)Borrare toda la informacion de mi base de datos del chat cuando lleguemos al comentario 3000.")      
    except:           
          context.bot.send_message(chat_id=str(update.effective_user["id"]), text="Silvabot1.0, puede hacer:\n1)Informar todos los dias cual es el usuario que mas comento durante las 24 hs y el numero de comentarios que realizo, si es que comento.\n2)Puedo alamacenar toda la informacion de tipo texto del chat grupal en mi base de datos Sqlite3.0,esto con la finalidad de en un futuro poder aprender a dialogar con ustedes.\n3)Borrare toda la informacion de mi base de datos del chat cuando lleguemos al comentario 3000.")
def presentation(update,context):        
        try:            
          context.bot.send_message(chat_id=str(update.effective_chat["id"]), 
                              text='Hola soy Silvabot1.0, estoy escrito en python  3.7.9, mi creador Diego Enrique Silva.\n Me creo con la capacidad de poder generar estadisticas en el chat grupal.\n Saludos a todos ustedes.')        
        except:        
          context.bot.send_message(chat_id=str(update.effective_user["id"]), 
                              text='Hola soy Silvabot1.0, estoy escrito en python  3.7.9, mi creador Diego Enrique Silva.\n Me creo con la capacidad de poder generar estadisticas en el chat grupal.\n Saludos a todos ustedes.')   


def main():
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    # Make sure to set use_context=True to use the new context based callbacks
    # Post version 12 this will no longer be necessary
    updater = Updater(TOKEN, use_context=True)         
    j=updater.job_queue       
    #job_minute = j.run_repeating(callback_minute, interval=86400, first=1800) 
    job_minute = j.run_daily(callback_minute, time=datetime.time(hour=17,minute=54,second=35))#Son 3 horas mas que mi zona horaria 
    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    #dp.add_handler(CommandHandler("start", start))       
    dp.add_handler(CommandHandler("time", time)) 
    dp.add_handler(CommandHandler("help", help))            
    dp.add_handler(CommandHandler("info", info))     
    dp.add_handler(CommandHandler("presentation", presentation))       
    dp.add_handler(CommandHandler("pastillas_magicas", random))
    dp.add_handler(MessageHandler(Filters.text,recept_message))
    # on noncommand i.e message - echo the message on Telegram
    #dp.add_handler(MessageHandler(Filters.text, echo))
    
    # log all errors
    dp.add_error_handler(error)           
    run(updater)      


if __name__ == '__main__':
    main()