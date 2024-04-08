# -*- coding: utf-8 -*-
# Author: JZ
# Name: JZ's SPECIAL PLANE MONITOR BOT
# Version: 0.0.8

#################################
### Importing libraries
#################################

from flightradar24api import FlightRadar24API # local lib
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, ConversationHandler
import logging
from datetime import datetime
import threading
from time import sleep
import pytz
import pandas as pd
import os
import os.path
import math
import warnings
from environs import Env
from pathlib import Path
from astral import LocationInfo
from astral.sun import sun

#################################
### Setting up enviroments
#################################

warnings.simplefilter(action='ignore', category=FutureWarning)
fr_api = FlightRadar24API()

# Read enviromental variables from config file
env = Env()

config_folder_path = 'config/'

if os.path.exists(config_folder_path) == False:
    raise Exception('No config folder found!')
    
if os.path.isfile(config_folder_path + 'config.env') == False:
    raise Exception('No config file found!')

env.read_env('config/config.env')

#################################
### Setting up enviromental variables
#################################

# Define how often the notification check will run
notification_delay = math.ceil(env.float('NOTIFICATION_DELAY')*60) # in mintues

## Telegram setting
telegram_bot_token = env.str('TELEGRAM_BOT_TOKEN') # Define Telegram bot token
chat_id = env.str('CHAT_ID') # use raw data bot to obtain user chat id # Define Telegram chat id

## Flightradar setting
airport_code = env.str('AIRPORT_CODE') # Define airport and local time zone

# Find location of the local airport
try:
    airport = fr_api.get_airport_details(code = airport_code)
    airport_name = airport['airport']['pluginData']['details']['name']
    airport_iata = airport['airport']['pluginData']['details']['code']['iata']
    airport_icao = airport['airport']['pluginData']['details']['code']['icao']
    airport_tz = airport['airport']['pluginData']['details']['timezone']['name']
    airport_lat = airport['airport']['pluginData']['details']['position']['latitude']
    airport_lon = airport['airport']['pluginData']['details']['position']['longitude']
except:
    sleep(60)

pages = list(range(1,(math.ceil(env.float('ENTRY_OBTAINED')/100)+1))) # defines the number of entries obtained in each run

## Speical Livery filter setting
livery_history_time_interval = math.ceil(env.float('SPECIAL_LIVERY_TIME_INTERVAL')) # Define the time interval between the same special livery plane is notified, in hours
livery_days = env.list("SPECIAL_LIVERY_NOTIFICATION_DAYS") # Days of the week when special livery will be notified; when empty notifications are sent on all days
livery_time = env.str('SPECIAL_LIVERY_NOTIFICATION_TIME')
sp_keywords = env.list("SPECIAL_LIVERY_KEYWORDS") # Filter workds to check for special livery

## Rare Plane filter setting
rare_plane_history_time_interval = math.ceil(env.float('RARE_PLANE_TIME_INTERVAL')) # in days
rare_plane_days = env.list("RARE_PLANE_NOTIFICATION_DAYS")
rare_plane_time = env.str('RARE_PLANE_NOTIFICATION_TIME')

## Rego watchlist setting
rego_watchlist_history_time_interval = math.ceil(env.float('REGO_WATCHLIST_TIME_INTERVAL')) # in hours
rego_watchlist_days = env.list("REGO_WATCHLIST_NOTIFICATION_DAYS")
rego_watchlist_time = env.str('REGO_WATCHLIST_NOTIFICATION_TIME')

## Type watchlist setting
type_watchlist_history_time_interval = math.ceil(env.float('TYPE_WATCHLIST_TIME_INTERVAL')) # in hours
type_watchlist_days = env.list("TYPE_WATCHLIST_NOTIFICATION_DAYS")
type_watchlist_time = env.str('TYPE_WATCHLIST_NOTIFICATION_TIME')

## File locations
# Filter name
exclusion_list_name = env.str('EXCLUSION_LIST_FILE_NAME')
livery_history_name = env.str('SPECIAL_LIVERY_HISTORY_FILE_NAME')
rare_plane_history_name = env.str('RARE_PLANE_HISTORY_FILE_NAME')
rego_watchlist_name = env.str('REGO_WATCHLIST_FILE_NAME')
type_watchlist_name = env.str('TYPE_WATCHLIST_FILE_NAME')
notifi_record_name = env.str('NOTIFICATION_RECORD_FILE_NAME')

# Create filter and define file path
filter_folder_path =  'config/filters/'
if os.path.exists(filter_folder_path) == False:
    os.mkdir(filter_folder_path)

exclusion_list_path = filter_folder_path + exclusion_list_name + '.csv'
livery_history_path = filter_folder_path + livery_history_name + '.csv'
rare_plane_history_path = filter_folder_path + rare_plane_history_name + '.csv'
rego_watchlist_path = filter_folder_path + rego_watchlist_name + '.csv'
type_watchlist_path = filter_folder_path + type_watchlist_name + '.csv'
notifi_record_path = filter_folder_path + notifi_record_name + '.csv'

#################################
### Utility functions
#################################

# Check the current flight status, return flight status as a str and current time as int
def check_flight_status (flight_details):
    departure_time = None
    land_time = None
    current_time = int(datetime.now().timestamp())
    
    if flight_details['time']['real']['departure'] is not None:
        departure_time = int(flight_details['time']['real']['departure'])
        
    if flight_details['time']['real']['arrival'] is not None:
        land_time = int(flight_details['time']['real']['arrival'])
    
    try:
        if land_time is not None:
            flight_status = 'Landed'
        elif departure_time is None:
            flight_status = 'On Ground'
        elif departure_time <= current_time:
            flight_status = 'In Flight'
    except:
        flight_status = 'N/A'
    
    return flight_status,current_time

# Find image link to an aircraft rego
def find_rego_details(registration_number):
    try:
        rego_details = fr_api.get_rego_details(registration_number)
        return rego_details
    except:
        return None
    
# Find a given rego's next flight
def check_next_flight(rego_details, airport_iata, airport_tz):
    if rego_details['data'] is not None:
        rego_flights = rego_details['data']
    else:
        return None, None, None, None
    
    for flight in rego_flights:
        if flight['airport']['origin']['code']['iata'] is not None:
            if flight['airport']['origin']['code']['iata'] == airport_iata and flight['time']['real']['departure'] == None:
                if flight['time']['scheduled']['departure'] is not None:
                    departure_time = datetime.fromtimestamp(flight['time']['scheduled']['departure']).astimezone(pytz.timezone(airport_tz))
                else:
                    departure_time = None
                    
                departure_airport_name = flight['airport']['destination']['name']
                departure_airport_iata = flight['airport']['destination']['code']['iata']
                departure_airport_icao = flight['airport']['destination']['code']['icao']
                break
            else:
                return None, None, None, None
            
    return departure_time, departure_airport_name, departure_airport_iata, departure_airport_icao

def check_flight_arrival_time (flight_details, airport_tz, airport_lat, airport_lon):
    region, city = airport_tz.split('/')
    airport_location = LocationInfo(city, region, airport_tz, airport_lat, airport_lon)
    
    if flight_details['time']['estimated']['arrival'] is not None:
        arrival_time = flight_details['time']['estimated']['arrival']
    elif flight_details['time']['scheduled']['arrival'] is not None:
        arrival_time = flight_details['time']['scheduled']['arrival']
    
    pytz_timezone = pytz.timezone(airport_tz)
    arrival_date_time = datetime.fromtimestamp(arrival_time, pytz_timezone)
    arrival_date = arrival_date_time.date()
    
    sun_info = sun(airport_location.observer, date = arrival_date, tzinfo = airport_location.timezone)
    dawn_time = int(sun_info['dawn'].timestamp())
    dusk_time = int(sun_info['dusk'].timestamp())
    
    try:
        if arrival_time > dawn_time and dusk_time > arrival_time:
            arrival_period = 'Daylight Arrival'
        else:
            arrival_period = 'Night-time Arrival'
    except:
        arrival_period = 'N/A'
    
    return arrival_period

# Format push notification content
def format_flight_details(flight_details, registration_number ,notif_type, rego_details, airport_iata, airport_icao):

    try:
        formatted_info = f"<b>{notif_type}</b>\n"
    except (KeyError, TypeError):
        formatted_info = "<b>Flight Details:</b>\n"
    
    try:
        formatted_info += f"  Flight number: {flight_details['identification']['number']['default']}\n"
    except (KeyError, TypeError):
        formatted_info += "  Flight number: N/A\n"
    
    try:
        formatted_info += f"  Dep. Airport: {flight_details['airport']['origin']['name']} ({flight_details['airport']['origin']['code']['iata']}/{flight_details['airport']['origin']['code']['icao']})\n"
    except:
        formatted_info += f"  Dep. Airport: N/A\n"
    
    try:
        flight_status, current_time = check_flight_status(flight_details)
        formatted_info += (f'  Status: {flight_status}\n')
    except (TypeError):
        formatted_info += f"  Status: N/A\n"
        
    try:
        formatted_info += f"  Aircraft Model: {flight_details['aircraft']['model']['text']} ({flight_details['aircraft']['model']['code']})\n"
    except (KeyError, TypeError):
        formatted_info += "  Aircraft Model: N/A\n"
        
    try:
        formatted_info += f"  Registration: {flight_details['aircraft']['registration']}\n"
    except (KeyError, TypeError):
        formatted_info += "  Registration: N/A\n"

    try:
        formatted_info += f"  Airline: {flight_details['airline']['name']} " \
                          f"({flight_details['airline']['code']['iata']}" \
                          f"/{flight_details['airline']['code']['icao']})\n\n"
    except (KeyError, TypeError):
        formatted_info += "  Airline: N/A\n\n"

    formatted_info += "<b>Arrival Details:</b>\n"
    
    try:
        arrival_period = check_flight_arrival_time(flight_details, airport_tz, airport_lat, airport_lon)
        formatted_info += f"  Arrival Period: {arrival_period}\n"
    except:
        formatted_info += f"  Arrival Period: N/A\n"
    
    try:
        scheduled_arrival = datetime.fromtimestamp(flight_details['time']['scheduled']['arrival']).astimezone(pytz.timezone(airport_tz))
        formatted_info += f"  Scheduled Arrival: {scheduled_arrival.strftime('%a %H:%M')} (Local)\n"
    except (KeyError, TypeError, OSError):
        formatted_info += "  Scheduled Arrival: N/A\n"

    try:
        estimated_arrival = datetime.fromtimestamp(flight_details['time']['estimated']['arrival']).astimezone(pytz.timezone(airport_tz))
        formatted_info += f"  Estimated Arrival: {estimated_arrival.strftime('%a %H:%M')} (Local)\n"
    except (KeyError, TypeError, OSError):
        formatted_info += "  Estimated Arrival: N/A\n"

    departure_time, departure_airport_name, departure_airport_iata, departure_airport_icao = check_next_flight(rego_details, airport_iata, airport_tz)
    
    if departure_time is not None:
        
        formatted_info += "\n<b>Next Flight Details:</b>\n"
        
        try:
            formatted_info += f"  Est. Departure: {departure_time.strftime('%a %H:%M')} (Local)\n"
        except (KeyError, TypeError, OSError):
            formatted_info += "  Est. Departure: N/A\n"
        
        try:
            formatted_info += f"  Dest. Airport: {departure_airport_name} ({departure_airport_iata}/{departure_airport_icao})\n"
        except (KeyError, TypeError, IndexError):
            formatted_info += "  Dest. Airport: N/A\n"

    try:
        if flight_details['identification']['id'] is None:
            link_section = 'data/flights/' + flight_details['identification']['number']['default']
        else:
            link_section = flight_details['identification']['id']
        formatted_info += f"\nhttps://www.flightradar24.com/{link_section}\n\n"
    except (KeyError, TypeError, IndexError):
        pass

    return formatted_info

# Convert a dataframe to text to be displayed by telegram
def convert_df_text(selected_filter, pass_filter, show_index):
    num_of_entries = len(pass_filter)
    col_names = list(pass_filter.columns)
    
    if num_of_entries == 0:
        table_text = '<b>' + selected_filter + ' is empty! </b>' 
        return table_text, num_of_entries
    
    table_text = '<b>' + selected_filter + '</b>'
        
    for item in list(range(num_of_entries)):
        if show_index == True:
            table_text += '\n\nIndex: ' + str(item)
        else:
            table_text = table_text + '\n'
        
        for col_name in col_names:
            if col_name != 'Time':
                table_text += '\n' + col_name + ': ' + str((pass_filter[col_name].iloc[item]))
    return table_text, num_of_entries


# Record the pushed notification
def record_notification (flight_details, registration_number, notifi_record_path):
    flight_status, current_time = check_flight_status(flight_details)
    
    if flight_status == 'On Ground':
        if os.path.isfile(notifi_record_path) == False:
            flight_entry = {'Registration':registration_number,'Flight Status':flight_status,'Time':current_time}
            df_notifi_record = pd.DataFrame(data = flight_entry,index = [0])
            df_notifi_record.to_csv(notifi_record_path,index = False)
            
        else:
            df_notifi_record = pd.read_csv(notifi_record_path,header = 0)
            
            
            if (registration_number in df_notifi_record['Registration'].values) == True:
                    
                    rego_location = df_notifi_record.index[df_notifi_record['Registration'] == registration_number].tolist()
                    
                    if len(rego_location) != 0:
                        df_notifi_record.loc[rego_location[0],'Time'] = current_time
                        df_notifi_record.to_csv(notifi_record_path,index = False)
            else:
                flight_entry = {'Registration':registration_number,'Flight Status':flight_status,'Time':current_time}
                df_notifi_record_new_entry = pd.DataFrame(data = flight_entry,index = [0])
                df_notifi_record = pd.concat([df_notifi_record,df_notifi_record_new_entry], ignore_index = True)
                df_notifi_record.to_csv(notifi_record_path,index = False)

# Build a rare plane history database wtih a fresh installation
def build_rare_plane_history (airport_code, rare_plane_history_path):
    current_page = 1
    total_page = 1
    
    try:
        while current_page <= total_page:
            airport = fr_api.get_airport_details(code = airport_code, page = -current_page)
            total_page = airport['airport']['pluginData']['schedule']['arrivals']['page']['total']
            current_page += 1
            
            airport_arrival_history = airport['airport']['pluginData']['schedule']['arrivals']['data']
            
            for arrived_flight in airport_arrival_history:
                if arrived_flight['flight']['owner'] is not None:
                    airline_name = arrived_flight['flight']['owner']['code']['icao']
                else:
                    continue
        
                if arrived_flight['flight']['aircraft'] is not None:
                    aircraft_type = arrived_flight['flight']['aircraft']['model']['code']
                    flight_details = arrived_flight['flight']
                else:
                    continue
                
                if arrived_flight['flight']['time']['real']['arrival'] is not None:
                    arrived_time = arrived_flight['flight']['time']['real']['arrival']
                else:
                    continue
                
                if os.path.isfile(rare_plane_history_path) == False:
                    current_flight_data = {'Airline':airline_name,'Aircraft Type':aircraft_type,'Time':arrived_time}
                    df_rare_plane_history = pd.DataFrame(data = current_flight_data,index = [0])
                    df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
            
                else:
                    df_rare_plane_history = pd.read_csv(rare_plane_history_path)
                    
                    df_rare_airline = df_rare_plane_history.loc[df_rare_plane_history['Airline'] == airline_name]
                
                    if len(df_rare_airline) == 0:
                        current_flight_data = {'Airline':airline_name,'Aircraft Type':aircraft_type,'Time':arrived_time}
                        df_new_flight = pd.DataFrame(data = current_flight_data,index = [0])
                        df_rare_plane_history = pd.concat([df_rare_plane_history,df_new_flight], ignore_index = True)
                        df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
                    else:
                        df_rare_airline_aircraft = df_rare_airline.loc[df_rare_airline['Aircraft Type'] == aircraft_type]   
                        
                        if len(df_rare_airline_aircraft) == 0:
                            current_flight_data = {'Airline':airline_name,'Aircraft Type':aircraft_type,'Time':arrived_time}
                            df_new_flight = pd.DataFrame(data = current_flight_data,index = [0])
                            df_rare_plane_history = pd.concat([df_rare_plane_history,df_new_flight], ignore_index = True)
                            df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
    except Exception as e:
        sleep(60)

if os.path.isfile(rare_plane_history_path) == False:
    build_rare_plane_history (airport_code, rare_plane_history_path)

#################################
### Filter functions
#################################

# Function to check if a plane has been manually added to the exclusion list
def check_exclusion_list(exclusion_list_path,registration_number):
    if os.path.isfile(exclusion_list_path) == False:
        exclusion_list = {'Airline':'','Registration':'','Description':''}
        df_exclusion_list = pd.DataFrame(data = exclusion_list,index = [0])
        df_exclusion_list.to_csv(exclusion_list_path,index = False)
        return None
    else:
        df_exclusion_list = pd.read_csv(exclusion_list_path,header = 0)
        return registration_number in df_exclusion_list['Registration'].values

# Check if a plane has speical livery and notify the user if it's not in the exclusion list and has not been notified in the past x hours
def check_speical_livery(livery_history_path, sp_keywords, arriving_flight, livery_history_time_interval, livery_days, livery_time, exclusion_list_path):
    if arriving_flight['flight']['airline'] is not None:
        airline_name = arriving_flight['flight']['airline']['name']
    else:
        return None
    
    if arriving_flight['flight']['aircraft'] is not None:
        registration_number = arriving_flight['flight']['aircraft']['registration'] 
        flight_details = arriving_flight['flight']
    else:
        return None
    
    if len(livery_days) != 0:
        if arriving_flight['flight']['time'] is not None:
            arrival_day = datetime.fromtimestamp(arriving_flight['flight']['time']['scheduled']['arrival']).astimezone(pytz.timezone(airport_tz)).strftime('%a')
            if arrival_day not in livery_days: 
                return None
            
    if livery_time == 'Off':
        return None
    elif livery_time == 'Daylight':
        arrival_period = check_flight_arrival_time(arriving_flight['flight'], airport_tz, airport_lat, airport_lon)
        if arrival_period != 'Daylight Arrival':
            return None

    if any(key in airline_name for key in sp_keywords):
        if check_exclusion_list(exclusion_list_path, registration_number) == False:
            if os.path.isfile(livery_history_path) == False:
                current_flight_data = {'Registration':registration_number,'Time':int(datetime.now().timestamp())}
                df_livery_history = pd.DataFrame(data = current_flight_data,index = [0])
                df_livery_history.to_csv(livery_history_path,index = False)
                return [flight_details, registration_number]
            else:
                df_livery_history = pd.read_csv(livery_history_path)
                
                rego_location = df_livery_history.index[df_livery_history['Registration'] == registration_number].tolist()
                
                if len(rego_location) != 0:
                    calculated_time_interval = (int(datetime.now().timestamp()) - int(df_livery_history.loc[rego_location[0],'Time']))/(60*60)
                    
                    if calculated_time_interval > livery_history_time_interval:
                        df_livery_history.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                        df_livery_history.to_csv(livery_history_path,index = False)
                        return [flight_details, registration_number]
                    else:
                        df_livery_history.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                        df_livery_history.to_csv(livery_history_path,index = False)
                        return None
                else:
                    df_new_flight = pd.DataFrame({'Registration':registration_number,'Time':int(datetime.now().timestamp())},index = [0])
                    df_livery_history = pd.concat([df_livery_history,df_new_flight], ignore_index = True)
                    df_livery_history.to_csv(livery_history_path,index = False)
                    return [flight_details, registration_number]
        else:
            return None
    else:
        return None

# Check if an aircraft type or airline has been in the airport in the past x days
def check_rare_plane (rare_plane_history_path, arriving_flight, rare_plane_history_time_interval, rare_plane_days, rare_plane_time, exclusion_list_path):
    if arriving_flight['flight']['owner'] is not None:
        airline_name = arriving_flight['flight']['owner']['code']['icao']
    else:
        return None
    
    if arriving_flight['flight']['aircraft'] is not None:
        aircraft_type = arriving_flight['flight']['aircraft']['model']['code']
        registration_number = arriving_flight['flight']['aircraft']['registration'] 
        flight_details = arriving_flight['flight']
    else:
        return None
    
    if len(rare_plane_days) != 0:
        if arriving_flight['flight']['time'] is not None:
            arrival_day = datetime.fromtimestamp(arriving_flight['flight']['time']['scheduled']['arrival']).astimezone(pytz.timezone(airport_tz)).strftime('%a')
            if arrival_day not in rare_plane_days: 
                return None
    
    if rare_plane_time == 'Off':
        return None
    elif rare_plane_time == 'Daylight':
        arrival_period = check_flight_arrival_time(arriving_flight['flight'], airport_tz, airport_lat, airport_lon)
        if arrival_period != 'Daylight Arrival':
            return None

    if check_exclusion_list(exclusion_list_path, registration_number) == False:  
        if os.path.isfile(rare_plane_history_path) == False:
            current_flight_data = {'Airline':airline_name,'Aircraft Type':aircraft_type,'Time':int(datetime.now().timestamp())}
            df_rare_plane_history = pd.DataFrame(data = current_flight_data,index = [0])
            df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
            return [flight_details, registration_number]
        else:
            df_rare_plane_history = pd.read_csv(rare_plane_history_path)
            
            df_rare_airline = df_rare_plane_history.loc[df_rare_plane_history['Airline'] == airline_name]
            
            if len(df_rare_airline) == 0:
                current_flight_data = {'Airline':airline_name,'Aircraft Type':aircraft_type,'Time':int(datetime.now().timestamp())}
                df_new_flight = pd.DataFrame(data = current_flight_data,index = [0])
                df_rare_plane_history = pd.concat([df_rare_plane_history,df_new_flight], ignore_index = True)
                df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
                return [flight_details, registration_number]
            else:
                df_rare_airline_aircraft = df_rare_airline.loc[df_rare_airline['Aircraft Type'] == aircraft_type]   
                
                if len(df_rare_airline_aircraft) == 0:
                    current_flight_data = {'Airline':airline_name,'Aircraft Type':aircraft_type,'Time':int(datetime.now().timestamp())}
                    df_new_flight = pd.DataFrame(data = current_flight_data,index = [0])
                    df_rare_plane_history = pd.concat([df_rare_plane_history,df_new_flight], ignore_index = True)
                    df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
                    return [flight_details, registration_number]
                else:
                    calculated_time_interval =  (int(datetime.now().timestamp()) - int(df_rare_airline_aircraft['Time'].max()))/(60*60*24)
                    
                    if calculated_time_interval > rare_plane_history_time_interval:
                        df_rare_plane_history.loc[(df_rare_plane_history['Airline'] == airline_name) & (df_rare_plane_history['Aircraft Type'] == aircraft_type) 
                                                  & (df_rare_plane_history['Time'] == int(df_rare_airline_aircraft['Time'].max())),['Time']]= int(datetime.now().timestamp())
                        df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
                        return [flight_details, registration_number]
                    else:
                        df_rare_plane_history.loc[(df_rare_plane_history['Airline'] == airline_name) & (df_rare_plane_history['Aircraft Type'] == aircraft_type) 
                                                  & (df_rare_plane_history['Time'] == int(df_rare_airline_aircraft['Time'].max())),['Time']]= int(datetime.now().timestamp())
                        df_rare_plane_history.to_csv(rare_plane_history_path,index = False)
                        return None
    else:
        return None

# Check if a rego is in the watchlist                    
def check_rego_watchlist(rego_watchlist_path, arriving_flight, rego_watchlist_history_time_interval, rego_watchlist_days, rego_watchlist_time, exclusion_list_path):
    if arriving_flight['flight']['aircraft'] is not None:
        registration_number = arriving_flight['flight']['aircraft']['registration']
        flight_details = arriving_flight['flight']
    else:
        return None
    
    if len(rego_watchlist_days) != 0:
        if arriving_flight['flight']['time'] is not None:
            arrival_day = datetime.fromtimestamp(arriving_flight['flight']['time']['scheduled']['arrival']).astimezone(pytz.timezone(airport_tz)).strftime('%a')
            if arrival_day not in rego_watchlist_days: 
                return None
    
    if rego_watchlist_time == 'Off':
        return None
    elif rego_watchlist_time == 'Daylight':
        arrival_period = check_flight_arrival_time(arriving_flight['flight'], airport_tz, airport_lat, airport_lon)
        if arrival_period != 'Daylight Arrival':
            return None
    
    if check_exclusion_list(exclusion_list_path, registration_number) == False:
        if os.path.isfile(rego_watchlist_path) == False:
            rego_watchlist = {'Airline':'','Registration':'','Description':'','Time':''}
            df_rego_watchlist = pd.DataFrame(data = rego_watchlist,index = [0])
            df_rego_watchlist.to_csv(rego_watchlist_path,index = False)
            return None
        
        else:
            df_rego_watchlist = pd.read_csv(rego_watchlist_path,header = 0)
            
            if (registration_number in df_rego_watchlist['Registration'].values) == True:
                
                rego_location = df_rego_watchlist.index[df_rego_watchlist['Registration'] == registration_number].tolist()
                
                if len(rego_location) != 0:
                    if (math.isnan(df_rego_watchlist.loc[rego_location[0],'Time'])) == True:
                        df_rego_watchlist.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                        df_rego_watchlist.to_csv(rego_watchlist_path,index = False)
                        return[flight_details, registration_number]
                    else:
                        calculated_time_interval = (int(datetime.now().timestamp()) - int(df_rego_watchlist.loc[rego_location[0],'Time']))/(60*60)
                        
                        if calculated_time_interval > rego_watchlist_history_time_interval:
                            df_rego_watchlist.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                            df_rego_watchlist.to_csv(rego_watchlist_path,index = False)
                            return[flight_details, registration_number]
                        else:
                            df_rego_watchlist.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                            df_rego_watchlist.to_csv(rego_watchlist_path,index = False)
                            return None
                else: 
                    return None
            else: 
                    return None
    else:
        return None

# Check if an aircraft type or airline is in the watchlist    
def check_type_watchlist(type_watchlist_path, arriving_flight, type_watchlist_history_time_interval, type_watchlist_days, type_watchlist_time, exclusion_list_path):
    if arriving_flight['flight']['owner'] is not None:
        airline_name = arriving_flight['flight']['owner']['code']['icao']
    else:
        return None
    
    if arriving_flight['flight']['aircraft'] is not None:
        aircraft_type = arriving_flight['flight']['aircraft']['model']['code']
        registration_number = arriving_flight['flight']['aircraft']['registration'] 
        flight_details = arriving_flight['flight']
    else:
        return None
    
    if type_watchlist_time == 'Off':
        return None
    elif type_watchlist_time == 'Daylight':
        arrival_period = check_flight_arrival_time(arriving_flight['flight'], airport_tz, airport_lat, airport_lon)
        if arrival_period != 'Daylight Arrival':
            return None
    
    if len(type_watchlist_days) != 0:
        if arriving_flight['flight']['time'] is not None:
            arrival_day = datetime.fromtimestamp(arriving_flight['flight']['time']['scheduled']['arrival']).astimezone(pytz.timezone(airport_tz)).strftime('%a')
            if arrival_day not in type_watchlist_days: 
                return None
    
    if check_exclusion_list(exclusion_list_path, registration_number) == False:
        if os.path.isfile(type_watchlist_path) == False:
            type_watchlist = {'Airline':'','Aircraft Type':'','Time':''}
            df_type_watchlist = pd.DataFrame(data = type_watchlist,index = [0])
            df_type_watchlist.to_csv(type_watchlist_path,index = False)
            return None
        else:
            df_type_watchlist = pd.read_csv(type_watchlist_path,header = 0)
            
            if (airline_name in df_type_watchlist['Airline'].values) == True and (aircraft_type in df_type_watchlist['Aircraft Type'].values) == True:
                rego_location = df_type_watchlist.index[(df_type_watchlist['Airline'] == airline_name) & (df_type_watchlist['Aircraft Type'] == aircraft_type)].tolist()
                
                if len(rego_location) != 0:
                    if (math.isnan(df_type_watchlist.loc[rego_location[0],'Time'])) == True:
                        df_type_watchlist.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                        df_type_watchlist.to_csv(type_watchlist_path,index = False)
                        return[flight_details, registration_number]
                    else:
                        calculated_time_interval = (int(datetime.now().timestamp()) - int(df_type_watchlist.loc[rego_location[0],'Time']))/(60*60)
                        
                        if calculated_time_interval > type_watchlist_history_time_interval:
                            df_type_watchlist.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                            df_type_watchlist.to_csv(type_watchlist_path,index = False)
                            return[flight_details, registration_number]
                        else:
                            df_type_watchlist.loc[rego_location[0],'Time'] = int(datetime.now().timestamp())
                            df_type_watchlist.to_csv(type_watchlist_path,index = False)
                            return None
                else: 
                    return None
            else:
                return None
    else:
        return None

# Send notification when a notified plane has changed status
def check_record_notification (arriving_flight, notifi_record_path):
    if arriving_flight['flight']['aircraft'] is not None:
        registration_number = arriving_flight['flight']['aircraft']['registration'] 
        flight_details = arriving_flight['flight']
    else:
        return None

    flight_status, current_time = check_flight_status(flight_details)
    
    if check_exclusion_list(exclusion_list_path, registration_number) == False:
        if os.path.isfile(notifi_record_path) == False:
            return None
        else:
            df_notifi_record = pd.read_csv(notifi_record_path,header = 0)
            
            if (registration_number in df_notifi_record['Registration'].values) == True:
                rego_location = df_notifi_record.index[(df_notifi_record['Registration'] == registration_number)].tolist()
                
                if ((current_time - df_notifi_record.loc[rego_location[0],'Time'])/(60*60)) > 24:
                    df_notifi_record.drop(rego_location[0], inplace=True)
                    df_notifi_record.to_csv(notifi_record_path,index = False)
                    return None
                elif df_notifi_record.loc[rego_location[0],'Flight Status'] == 'On Ground' and flight_status == 'In Flight':
                    df_notifi_record.drop(rego_location[0], inplace=True)
                    df_notifi_record.to_csv(notifi_record_path,index = False)
                    return[flight_details, registration_number]
                else:
                    return None
                
            else:
                return None
    else:
        return None

#################################    
### Telegram Bot Functions       
#################################
             
# Main functions to send notifications
def send_notification(context: CallbackContext):
    try:
        print('Checking for updates...')
        for page in pages:
            airport = fr_api.get_airport_details(code = airport_code, page = page)
            airport_arrivals = airport['airport']['pluginData']['schedule']['arrivals']['data']
            for arriving_flight in airport_arrivals:
                special_livery_res = check_speical_livery(livery_history_path, sp_keywords, arriving_flight, livery_history_time_interval, livery_days, livery_time, exclusion_list_path)
                rare_plane_res = check_rare_plane(rare_plane_history_path, arriving_flight, rare_plane_history_time_interval, rare_plane_days, rare_plane_time, exclusion_list_path)
                rego_watchlist_res = check_rego_watchlist(rego_watchlist_path, arriving_flight, rego_watchlist_history_time_interval, rego_watchlist_days, rego_watchlist_time, exclusion_list_path)
                type_watchlist_res = check_type_watchlist(type_watchlist_path, arriving_flight, type_watchlist_history_time_interval, type_watchlist_days, type_watchlist_time, exclusion_list_path)
                status_change_res = check_record_notification(arriving_flight, notifi_record_path)
                
                res_flight_details = None
                res_registration_number = None
                res_notif_type = None
            
                if special_livery_res is not None:
                    res_flight_details = special_livery_res[0]
                    res_registration_number = special_livery_res[1]
                    res_notif_type = 'Special Livery'
                elif rare_plane_res is not None:
                    res_flight_details = rare_plane_res[0]
                    res_registration_number = rare_plane_res[1]
                    res_notif_type = 'Rare Plane/Airline'
                elif rego_watchlist_res is not None:
                    res_flight_details = rego_watchlist_res[0]
                    res_registration_number = rego_watchlist_res[1]
                    res_notif_type = 'Watchlist Registration'
                elif type_watchlist_res is not None:
                    res_flight_details = type_watchlist_res[0]
                    res_registration_number = type_watchlist_res[1]
                    res_notif_type = 'Watchlist Aircraft Type'
                elif status_change_res is not None:
                    res_flight_details = status_change_res[0]
                    res_registration_number = status_change_res[1]
                    res_notif_type = 'Status Change'
                
                if res_flight_details is not None and res_registration_number is not None and res_notif_type is not None:
                    rego_details = find_rego_details(res_registration_number)
                    
                    if rego_details is not None:
                        photo_url = rego_details['aircraftImages'][0]['images']['medium'][0]['link']
                    
                    flight_info = format_flight_details(res_flight_details, res_registration_number, res_notif_type, rego_details, airport_iata, airport_icao)

                    if photo_url is not None: 
                        context.bot.send_photo(chat_id=context.job.context, photo=photo_url, caption=f'Aircraft Photo: {res_registration_number}')
                    
                    context.bot.send_message(chat_id=context.job.context, text = flight_info, parse_mode='HTML')
                    record_notification(res_flight_details, res_registration_number, notifi_record_path)
                    
    except:
        print('Error when updating!')
        sleep(60)
                
## Telegram bot menu functions

FILTER_CHOICE, OP_CHOICE, ADD_ENTRY, ADD_ENTRY_RICH, DELETE_ENTRY = range(5)

# Define the main menu keyboard
filters_keyboard = [['Exclusion List', 'Rego Watchlist', 'Type Watchlist']]
filters_markup = ReplyKeyboardMarkup(filters_keyboard, resize_keyboard=True)

op_keyboard = [['Add Entry', 'Delete Entry', 'Exit']]
op_markup = ReplyKeyboardMarkup(op_keyboard, resize_keyboard=True)

empty_op_keyboard = [['Add Entry', 'Exit']]
empty_op_markup = ReplyKeyboardMarkup(empty_op_keyboard, resize_keyboard=True)

# Define a function to handle the start command
def start(update: Update, context: CallbackContext) -> int:
    update.message.reply_text(
        "Please choose the filter you would like to modify:",
        reply_markup=filters_markup
    )
    return FILTER_CHOICE

# Define a function to handle the user's choice from the main menu
def filter_choice(update: Update, context: CallbackContext) -> int:
    global user_choice
    global num_of_entries
    user_choice = update.message.text
    
    if user_choice == 'Exclusion List':
        df_exclusion_list = pd.read_csv(exclusion_list_path,header = 0)
        display_text, num_of_entries = convert_df_text(user_choice, df_exclusion_list, True)
        update.message.reply_html(display_text)
    elif user_choice == 'Rego Watchlist':
        df_rego_watchlist = pd.read_csv(rego_watchlist_path,header = 0)
        display_text, num_of_entries = convert_df_text(user_choice, df_rego_watchlist, True)
        update.message.reply_html(display_text)
    elif user_choice == 'Type Watchlist':
        df_type_watchlist = pd.read_csv(type_watchlist_path,header = 0)
        display_text, num_of_entries = convert_df_text(user_choice, df_type_watchlist, True)
        update.message.reply_html(display_text)
    else:
        update.message.reply_text("Invalid choice. Please select a filter from the menu.")
        return FILTER_CHOICE
    
    if num_of_entries == 0:
        update.message.reply_text("What would you like to do?:",reply_markup=empty_op_markup)
    else:
        update.message.reply_text("What would you like to do?:",reply_markup=op_markup)
        
    return OP_CHOICE

# Define a function to add a new entry to the DataFrame
def add_entry(update: Update, context: CallbackContext) -> int:
    if user_choice == 'Exclusion List':    
        update.message.reply_text("Copy a notification, or enter the ICAO code of the airline, registration number and description of the new entry, separated by comma (e.g., QFA,VH-XZP,Qantas Retro Roo):")
    elif user_choice == 'Rego Watchlist':
        update.message.reply_text("Copy a notification, or enter the ICAO code of the airline, registration number and description of the new entry, separated by comma (e.g., QFA,VH-XZP,Qantas Retro Roo):")
    elif user_choice == 'Type Watchlist':
        update.message.reply_text("Copy a notification, or enter the ICAO code of the airline and the aircraft type, separated by a comma (e.g., QFA,B744):")
    
    return ADD_ENTRY

# Define a function to handle adding a new entry
def add_new_entry(update: Update, context: CallbackContext) -> int:
    global new_entry_text
    new_entry_text = update.message.text.splitlines()
    
    if len(new_entry_text) != 1:
        update.message.reply_text("Please enter description!")
        return ADD_ENTRY_RICH
    
    else:
        new_entry = new_entry_text.split(',')
        if len(new_entry) == 3:
            if user_choice == 'Exclusion List':
                airline, rego, description = new_entry
                df_exclusion_list = pd.read_csv(exclusion_list_path,header = 0)
                
                new_entry_data = {'Airline':airline,'Registration':rego,'Description':description}
                df_new_entry_data = pd.DataFrame(data = new_entry_data,index = [0])
                
                new_entry_display_text, num_of_entries = convert_df_text('New Entry', df_new_entry_data, False)
                update.message.reply_html(new_entry_display_text)
                
                df_exclusion_list = pd.concat([df_exclusion_list,df_new_entry_data], ignore_index = True)
                
                new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_exclusion_list, True)
                update.message.reply_html(new_df_display_text)
                
                df_exclusion_list.to_csv(exclusion_list_path,index = False)
            
            elif user_choice == 'Rego Watchlist':
                airline, rego, description = new_entry
                df_rego_watchlist = pd.read_csv(rego_watchlist_path,header = 0)
                
                new_entry_data = {'Airline':airline,'Registration':rego,'Description':description,'Time':int(0)}
                df_new_entry_data = pd.DataFrame(data = new_entry_data,index = [0])
                
                new_entry_display_text, num_of_entries = convert_df_text('New Entry', df_new_entry_data, False)
                update.message.reply_html(new_entry_display_text)
                
                df_rego_watchlist = pd.concat([df_rego_watchlist,df_new_entry_data], ignore_index = True)
                
                new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_rego_watchlist, True)
                update.message.reply_html(new_df_display_text)
                
                df_rego_watchlist.to_csv(rego_watchlist_path,index = False)
            
            
            context.bot.send_message(chat_id=update.effective_chat.id, text="Operation Complete!")
            return ConversationHandler.END
        
        elif len(new_entry) == 2:
            if user_choice == 'Type Watchlist':
                airline, aircraft_type = new_entry
                df_type_watchlist = pd.read_csv(type_watchlist_path,header = 0)
                
                new_entry_data = {'Airline':airline,'Aircraft Type':aircraft_type,'Time':int(0)}
                df_new_entry_data = pd.DataFrame(data = new_entry_data,index = [0])
                
                new_entry_display_text, num_of_entries = convert_df_text('New Entry', df_new_entry_data, False)
                update.message.reply_html(new_entry_display_text)
                
                df_type_watchlist = pd.concat([df_type_watchlist,df_new_entry_data], ignore_index = True)
                
                new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_type_watchlist, True)
                update.message.reply_html(new_df_display_text)
                
                df_type_watchlist.to_csv(type_watchlist_path,index = False)
                
            context.bot.send_message(chat_id=update.effective_chat.id, text="Operation Complete!")
            return ConversationHandler.END
        else:
            update.message.reply_text("Invalid format. Please enter the information separated by comma.")
            return ADD_ENTRY

def add_new_entry_rich_text(update: Update, context: CallbackContext) -> int:
    rego = new_entry_text[5].replace('  Registration: ','')
    airline = ''.join(list(new_entry_text[6].split()[-1])[-4:-1])
    aircraft_type = ''.join(list(new_entry_text[4].split()[-1])[-5:-1])
    description = update.message.text
    
    if user_choice == 'Exclusion List':
        df_exclusion_list = pd.read_csv(exclusion_list_path,header = 0)
                
        new_entry_data = {'Airline':airline,'Registration':rego,'Description':description}
        df_new_entry_data = pd.DataFrame(data = new_entry_data,index = [0])
        
        new_entry_display_text, num_of_entries = convert_df_text('New Entry', df_new_entry_data, False)
        update.message.reply_html(new_entry_display_text)
        
        df_exclusion_list = pd.concat([df_exclusion_list,df_new_entry_data], ignore_index = True)
        
        new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_exclusion_list, True)
        update.message.reply_html(new_df_display_text)
        
        df_exclusion_list.to_csv(exclusion_list_path,index = False)
            
        context.bot.send_message(chat_id=update.effective_chat.id, text="Operation Complete!")
        return ConversationHandler.END
    
    elif user_choice == 'Rego Watchlist':
        df_rego_watchlist = pd.read_csv(rego_watchlist_path,header = 0)
        
        new_entry_data = {'Airline':airline,'Registration':rego,'Description':description,'Time':int(0)}
        df_new_entry_data = pd.DataFrame(data = new_entry_data,index = [0])
        
        new_entry_display_text, num_of_entries = convert_df_text('New Entry', df_new_entry_data, False)
        update.message.reply_html(new_entry_display_text)
        
        df_rego_watchlist = pd.concat([df_rego_watchlist,df_new_entry_data], ignore_index = True)
        
        new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_rego_watchlist, True)
        update.message.reply_html(new_df_display_text)
        
        df_rego_watchlist.to_csv(rego_watchlist_path,index = False)
        
        context.bot.send_message(chat_id=update.effective_chat.id, text="Operation Complete!")
        return ConversationHandler.END
        
    elif user_choice == 'Type Watchlist':
        df_type_watchlist = pd.read_csv(type_watchlist_path,header = 0)
            
        new_entry_data = {'Airline':airline,'Aircraft Type':aircraft_type,'Time':int(0)}
        df_new_entry_data = pd.DataFrame(data = new_entry_data,index = [0])
                
        new_entry_display_text, num_of_entries = convert_df_text('New Entry', df_new_entry_data, False)
        update.message.reply_html(new_entry_display_text)
        
        df_type_watchlist = pd.concat([df_type_watchlist,df_new_entry_data], ignore_index = True)
        
        new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_type_watchlist, True)
        update.message.reply_html(new_df_display_text)
        
        df_type_watchlist.to_csv(type_watchlist_path,index = False)
                
        context.bot.send_message(chat_id=update.effective_chat.id, text="Operation Complete!")
        return ConversationHandler.END

# Define a function to delete an entry from the DataFrame
def delete_entry(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Enter the index of the entry you want to delete. To delete multiple indexes, seperate them with comma (e.g., 1,4,6):")
    return DELETE_ENTRY

# Define a function to handle deleting an entry
def delete_existing_entry(update: Update, context: CallbackContext) -> int:
    try:
        if ',' in update.message.text:
            delete_indexes = [int(x) for x in update.message.text.split(',')]
        else:
            delete_indexes = [int(update.message.text)]
    except ValueError:
        update.message.reply_text("Invalid input. Please enter a valid index.")
        return DELETE_ENTRY        
    
        
    try:
        if user_choice == 'Exclusion List':
            list_path = exclusion_list_path
        elif user_choice == 'Rego Watchlist':
            list_path = rego_watchlist_path
        elif user_choice == 'Type Watchlist':
            list_path = type_watchlist_path
        
        df_list = pd.read_csv(list_path,header = 0)
        df_deleted_entry = df_list.iloc[delete_indexes]
            
        deleted_entry_display_text, num_of_entries = convert_df_text('Deleted Index(es)', df_deleted_entry, False)
        update.message.reply_html(deleted_entry_display_text)
            
        df_list.drop(delete_indexes, inplace=True)
            
        new_df_display_text, num_of_entries = convert_df_text(('Updated ' + user_choice), df_list, True)
        update.message.reply_html(new_df_display_text)
             
        df_list.to_csv(list_path,index = False)
            
    except (ValueError, IndexError):
        update.message.reply_text("Invalid input. Please enter a valid index.")
        return DELETE_ENTRY
    
    context.bot.send_message(chat_id=update.effective_chat.id, text="Operation Complete!")        
    return ConversationHandler.END  # End the conversation

# Define a function to end the conversation
def end_conversation(update: Update, context: CallbackContext) -> int:
    context.bot.send_message(chat_id=update.effective_chat.id, text="Aborted!")
    return ConversationHandler.END

## Start the telegram bot
def main():
    # Initialize the Telegram Bot
    updater = Updater(telegram_bot_token, use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # Register command handlers
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('filters', start)],
        states={
            FILTER_CHOICE: [MessageHandler(Filters.text & ~Filters.command, filter_choice)],
            OP_CHOICE: [
                MessageHandler(Filters.regex('Add Entry'), add_entry),
                MessageHandler(Filters.regex('Delete Entry'), delete_entry),
                MessageHandler(Filters.regex('Exit'), end_conversation)
            ],
            ADD_ENTRY: [MessageHandler(Filters.text & ~Filters.command, add_new_entry)],
            ADD_ENTRY_RICH: [MessageHandler(Filters.text & ~Filters.command, add_new_entry_rich_text)],
            DELETE_ENTRY: [MessageHandler(Filters.text & ~Filters.command, delete_existing_entry)]
        },
        fallbacks=[]
    )

    # Register the ConversationHandler with the dispatcher
    dp.add_handler(conv_handler)
    # Start the Bot
    updater.start_polling()

    # Schedule periodic updates (every 60 seconds in this example)
    job_queue = updater.job_queue
    job_queue.run_repeating(send_notification, interval=notification_delay, first=0, context=chat_id)  # Replace 123456789 with your chat ID

    # Run the bot until you press Ctrl-C
    updater.idle()

if __name__ == '__main__':
    main()

