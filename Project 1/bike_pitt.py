import argparse
import collections
import csv
import json
import glob
import math
import os
import pandas
import re
import requests
import string
import sys
import time
import xml

class Bike():
    """
        Initialize the Bike class
        @Param baseURL: Base url to add extensions to
        @Param station_info: URL extension for general information on the station
        @Param station_status: URL extension for information on station bike occupancy
    """
    def __init__(self, baseURL, station_info, station_status):
        # Get the JSON data from each URL and retrieve the lists of station data
        self.station_data = requests.get(baseURL + station_info).json()['data']['stations']
        self.station_status_data = requests.get(baseURL + station_status).json()['data']['stations']
    
    """
        Returns the total number of bikes available in the system
        @Return Sum of all available bikes from all stations
    """
    def total_bikes(self):
        num_bikes = 0
        # Loop through the station status data and add each # of bikes available to the total
        for data in self.station_status_data:
            curr_bikes = data['num_bikes_available']
            num_bikes = num_bikes + curr_bikes
        return num_bikes

    """
        Returns the total number of docks available in the system
        @Return Sum of all available docks from all stations
    """
    def total_docks(self):
        num_docks = 0
        # Loop through the station status data and add each # of docks available to the total
        for data in self.station_status_data:
            curr_docks = data['num_docks_available']
            num_docks = num_docks + curr_docks
        return num_docks

    """
        Calculates the percentage of available docks at a given station
        @Param station_id: ID of the station to evaluate
        @Return: Percentage of all docks currently not in use
    """
    def percent_avail(self, station_id):
        curr_bikes = -1
        curr_docks = -1
        # Find the station matching the given station ID. If we find a match, record the number of docks and bikes available
        for data in self.station_status_data:
            if int(data['station_id']) == station_id:
                curr_docks = data['num_docks_available']
                curr_bikes = data['num_bikes_available']

        # If the station was not found, return an empty string
        if curr_bikes == -1 or curr_docks == -1:
            return ''
        
        # Calculate the percentage using available_docks / available_docks + current_bikes. Take that decimal and multiply it by 100, then get the floor of that number.
        # Once the number is calculated, convert it to a string and add a percent sign after it
        return str(math.floor((curr_docks / (curr_docks + curr_bikes)) * 100)) + '%'

    """
        Returns the 3 closest stations to a given location
        @Param latitude: Latitude of location
        @Param longitude: Longitude of location
        @Return: Dictionary of 3 closest staitons containing their IDs and names
    """
    def closest_stations(self, latitude, longitude):
        # Temporary array with the current locations' distances
        curr_distances = [float(math.inf), float(math.inf), float(math.inf)]
        # Array with dictionaries containing the data of each current closest location. 
        # The indices in the distances array should correspond to the indices of each dictionary in the locations array
        curr_locations = [{}, {}, {}]
        for data in self.station_data:
            # Calculate the current farthest location
            highest_distance = max(curr_distances)
            # Calculate the distance for the current station
            curr_distance = self.distance(data['lat'], data['lon'], latitude, longitude)
            # If the current station is closer than the farthest station, replace it's distance and station data
            if curr_distance < highest_distance:
                curr_locations[curr_distances.index(highest_distance)] = {data['station_id']: data['name']}
                curr_distances[curr_distances.index(highest_distance)] = curr_distance
        # Return the location array once finished
        end_dict = {}
        # Merge the 3 dictionaries before returning
        for location in curr_locations:
            end_dict = {**end_dict, **location}

        return end_dict

    """
        Calculate the closest station to the given location that has bikes available
        @Param latitude: Latitude of the current location
        @Param longitude: Longitude of the current location
        @Return: Dictionary containing the ID and name of the closest station that has bikes available
    """
    def closest_bike(self, latitude, longitude):
        curr_distance = float(math.inf)
        curr_location = {}
        for data in self.station_data:
            bikes_available = 0
            # Ensure the current station has bikes available before doing anything else
            for station_status in self.station_status_data:
                if station_status['station_id'] == data['station_id']:
                    bikes_available = int(station_status['num_bikes_available'])
            # If there are no bikes available, skip this entry
            if bikes_available > 0:
                # For each station, calculate its distance from the given coordinates
                temp_distance = self.distance(data['lat'], data['lon'], latitude, longitude)
                # Update the current station and distance if our current station is closer and has bikes
                if temp_distance < curr_distance:
                    curr_distance = temp_distance
                    curr_location = {data['station_id']: data['name']}
        return curr_location
    

    """
        Returns the station ID and number of bikes at a given station based on location
        @Param latitude: Latitude of the given location
        @Param longitude: Longitude of the given location
        @Return: Dictionary containing the station ID and number of bikes corresponding to the station at the latitude and longitude. Empty string if no station found
    """
    def station_bike_avail(self, latitude, longitude):
        station_id = -1
        end_dict = {}
        # Attempt to find the station matching the given latitude and longitude coordinates
        for data in self.station_data:
            if float(data['lat']) == latitude and float(data['lon']) == longitude:
                station_id = data['station_id']

        # Return an empty dictionary if no station matches the given location
        if station_id == -1:
            return end_dict

        # Once we have a matching station, locate its entry in the station status data and add the station ID and number of bikes available to our dictionary
        for data in self.station_status_data:
            if data['station_id'] == station_id:
                end_dict = {data['station_id']: data['num_bikes_available']}
        return end_dict        

    def distance(self, lat1, lon1, lat2, lon2):
        p = 0.017453292519943295
        a = 0.5 - math.cos((lat2-lat1)*p)/2 + math.cos(lat1*p)*math.cos(lat2*p) * (1-math.cos((lon2-lon1)*p)) / 2
        return 12742 * math.asin(math.sqrt(a))


# testing and debugging the Bike class

if __name__ == '__main__':
    instance = Bike('https://api.nextbike.net/maps/gbfs/v1/nextbike_pp/en', '/station_information.json', '/station_status.json')
    print('------------------total_bikes()-------------------')
    t_bikes = instance.total_bikes()
    print(type(t_bikes))
    print(t_bikes)
    print()

    print('------------------total_docks()-------------------')
    t_docks = instance.total_docks()
    print(type(t_docks))
    print(t_docks)
    print()

    print('-----------------percent_avail()------------------')
    p_avail = instance.percent_avail(342849) # replace with station ID
    print(type(p_avail))
    print(p_avail)
    print()

    print('----------------closest_stations()----------------')
    c_stations = instance.closest_stations(40.444618, -79.954707) # replace with latitude and longitude
    print(type(c_stations))
    print(c_stations)
    print()

    print('-----------------closest_bike()-------------------')
    c_bike = instance.closest_bike(40.444618, -79.954707) # replace with latitude and longitude
    print(type(c_bike))
    print(c_bike)
    print()

    print('---------------station_bike_avail()---------------')
    s_bike_avail = instance.station_bike_avail(40.438761, -79.997436) # replace with exact latitude and longitude of station
    print(type(s_bike_avail))
    print(s_bike_avail)
