# Author: Rushikesh Joshi
# Title - Public Transit Data Analytics Tool
# Description - This program interacts with the CTA2 L daily ridership database through a console-based interface. 
#               It allows users to input commands and retrieve data, using SQL for data retrieval and computations, while Python is used for displaying results and plotting data when requested. 
#               The project aims to provide insights into the CTA's daily ridership patterns.
# References - Piazza, Class Resources


import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import math

# Prints general statistics about the CTA L ridership database
def print_stats(dbConn):
    dbCursor = dbConn.cursor()
    stats_queries = [
        ("Select count(*) From Stations;", "  # of stations:"),
        ("Select count(*) From Stops;", "  # of stops:"),
        ("Select count(*) From Ridership;", "  # of ride entries:"),
        ("Select min(Ride_Date), max(Ride_Date) From Ridership;", "  date range:"),
        ("Select sum(Num_Riders) From Ridership;", "  Total ridership:")
    ]
    
    print("** Welcome to CTA L analysis app **\n")
    print("General Statistics:")
    
    for query, label in stats_queries:
        dbCursor.execute(query)
        result = dbCursor.fetchone()
        if "date range" in label:
            min_date = result[0].split(" ")[0]  # Keep only the date part
            max_date = result[1].split(" ")[0]  # Keep only the date part
            print(f"{label} {min_date} - {max_date}")
        else:
            print(f"{label} {result[0]:,}")

# Searches for station names that match a given partial name and prints the matching station IDs and names
def find_station_names(dbConn, partial_name):
    dbCursor = dbConn.cursor()
    dbCursor.execute("SELECT Station_ID, Station_Name FROM Stations WHERE Station_Name LIKE ? ORDER BY Station_Name", (partial_name,))
    stations = dbCursor.fetchall()
    if stations:
        for station_id, station_name in stations:
            print(f"{station_id} : {station_name}")
    else:
        print("**No stations found...")

# Calculates and prints the percentage of ridership for a specific station by type of day
def ridership_percentages(dbConn, station_name):
    dbCursor = dbConn.cursor()
    query = """
    SELECT Type_of_Day, SUM(Num_Riders)
    FROM Ridership
    JOIN Stations ON Ridership.Station_ID = Stations.Station_ID
    WHERE Station_Name = ?
    GROUP BY Type_of_Day
    ORDER BY CASE WHEN Type_of_Day = 'W' THEN 1 WHEN Type_of_Day = 'A' THEN 2 ELSE 3 END
    """
    dbCursor.execute(query, (station_name,))
    results = dbCursor.fetchall()
    
    if results:
        total_ridership = sum(count for _, count in results)
        print(f"Percentage of ridership for the {station_name} station:")
        for type_of_day, count in results:
            day_string = "Weekday" if type_of_day == 'W' else "Saturday" if type_of_day == 'A' else "Sunday/holiday"
            print(f"  {day_string} ridership: {count:,} ({(count / total_ridership) * 100:.2f}%)")
        print(f"  Total ridership: {total_ridership:,}")
    else:
        print("**No data found...")

# Computes and displays the total number of riders for weekdays for each station, along with the percentage of the total weekday ridership each station represents
def total_weekday_ridership(dbConn):
    dbCursor = dbConn.cursor()
    # Fetch the total number of riders for weekdays for each station
    query = """
    SELECT Station_Name, SUM(Num_Riders) as Total_Riders
    FROM Ridership
    JOIN Stations ON Ridership.Station_ID = Stations.Station_ID
    WHERE Type_of_Day = 'W'
    GROUP BY Stations.Station_ID
    ORDER BY Total_Riders DESC
    """
    dbCursor.execute(query)
    results = dbCursor.fetchall()
    
    # Calculate the total ridership on weekdays for all stations
    total_ridership = sum([count for _, count in results])
    
    print("Ridership on Weekdays for Each Station")
    for station_name, count in results:
        percentage = (count / total_ridership) * 100
        print(f"{station_name} : {count:,} ({percentage:.2f}%)")

# Lists all stops for a specific line color in a given direction, indicating whether each stop is handicap accessible
def list_stops_by_line_and_direction(dbConn, line_color):
    dbCursor = dbConn.cursor()
    line_color = line_color.title()

    dbCursor.execute("SELECT Line_ID FROM Lines WHERE Color = ?", (line_color,))
    line_exists = dbCursor.fetchone()
    if not line_exists:
        print("**No such line...")
        return

    direction = input("Enter a direction (N/S/W/E): ").upper()

    dbCursor.execute("""
    SELECT DISTINCT Direction FROM Stops
    JOIN StopDetails ON Stops.Stop_ID = StopDetails.Stop_ID
    JOIN Lines ON StopDetails.Line_ID = Lines.Line_ID
    WHERE Color = ?
    """, (line_color,))
    valid_directions = {row[0] for row in dbCursor.fetchall()}
    
    if direction not in valid_directions:
        print("**That line does not run in the direction chosen...")
        return

    dbCursor.execute("""
    SELECT Stop_Name, Direction, ADA
    FROM Stops
    JOIN StopDetails ON Stops.Stop_ID = StopDetails.Stop_ID
    JOIN Lines ON StopDetails.Line_ID = Lines.Line_ID
    WHERE Color = ? AND Direction = ?
    ORDER BY Stop_Name
    """, (line_color, direction))
    
    results = dbCursor.fetchall()
    if results:
        for stop_name, _, ada in results:
            ada_status = "(handicap accessible)" if ada else "(not handicap accessible)"  # Updated line
            print(f"{stop_name} : direction = {direction} {ada_status}")
    else:
        print("No stops found for this line and direction.")

# Calculates and prints the number of stops for each train line color by direction, including the percentage of total stops each represents
def stops_for_each_color_by_direction(dbConn):
    dbCursor = dbConn.cursor()
    # First, get the total number of stops from the Stops table
    dbCursor.execute("SELECT COUNT(*) FROM Stops")
    total_stops = dbCursor.fetchone()[0]
    
    # Fetch the number of stops for each color and direction
    query = """
    SELECT Lines.Color, Stops.Direction, COUNT(*) as Num_Stops
    FROM Stops
    JOIN StopDetails ON Stops.Stop_ID = StopDetails.Stop_ID
    JOIN Lines ON StopDetails.Line_ID = Lines.Line_ID
    GROUP BY Lines.Color, Stops.Direction
    ORDER BY Lines.Color ASC, Stops.Direction ASC
    """
    dbCursor.execute(query)
    results = dbCursor.fetchall()
    
    print("Number of Stops For Each Color By Direction")
    for color, direction, num_stops in results:
        percentage = (num_stops / total_stops) * 100
        print(f"{color} going {direction} : {num_stops} ({percentage:.2f}%)")

# Displays the total ridership for each year at a specified station, with an option for the user to plot this data
def yearly_ridership(dbConn, station_name):
    dbCursor = dbConn.cursor()
    # Check for exact or multiple station matches
    dbCursor.execute("SELECT DISTINCT Station_Name FROM Stations WHERE Station_Name LIKE ?", (station_name,))
    matching_stations = dbCursor.fetchall()
    if len(matching_stations) == 0:
        print("**No station found...")
        return
    elif len(matching_stations) > 1:
        print("**Multiple stations found...")
        return

    # Fetch the total ridership for each year
    query = """
    SELECT strftime('%Y', Ride_Date) as Year, SUM(Num_Riders) as Total_Riders
    FROM Ridership
    JOIN Stations ON Ridership.Station_ID = Stations.Station_ID
    WHERE Station_Name = ?
    GROUP BY Year
    ORDER BY Year
    """
    dbCursor.execute(query, (matching_stations[0][0],))
    results = dbCursor.fetchall()

    print(f"Yearly Ridership at {matching_stations[0][0]}")
    for year, total_riders in results:
        print(f"{year} : {total_riders:,}")

    # Ask user if they want to plot the data
    plot_input = input("\nPlot? (y/n) ")
    if plot_input.lower() == 'y':
        years = [row[0] for row in results]
        ridership = [row[1] for row in results]
        plt.figure(figsize=(10, 5))
        plt.plot(years, ridership)
        plt.title(f"Yearly Ridership at {matching_stations[0][0]} Station")
        plt.xlabel("Year")
        plt.ylabel("Number of Riders")
        plt.tight_layout()
        plt.show()

# Shows total ridership for each month of a specified year at a given station, with an option for plotting
def monthly_ridership(dbConn, station_name):
    dbCursor = dbConn.cursor()
    # Check for exact or multiple station matches
    dbCursor.execute("SELECT DISTINCT Station_Name FROM Stations WHERE Station_Name LIKE ?", (station_name,))
    matching_stations = dbCursor.fetchall()
    if len(matching_stations) == 0:
        print("**No station found...")
        return
    elif len(matching_stations) > 1:
        print("**Multiple stations found...")
        return

    year = input("Enter a year: ")

    # Fetch the total ridership for each month
    query = """
    SELECT strftime('%m', Ride_Date) as Month, SUM(Num_Riders) as Total_Riders
    FROM Ridership
    JOIN Stations ON Ridership.Station_ID = Stations.Station_ID
    WHERE Station_Name = ? AND strftime('%Y', Ride_Date) = ?
    GROUP BY Month
    ORDER BY Month
    """
    dbCursor.execute(query, (matching_stations[0][0], year))
    results = dbCursor.fetchall()

    print(f"Monthly Ridership at {matching_stations[0][0]} for {year}")
    for month, total_riders in results:
        print(f"{month}/{year} : {total_riders:,}")

    # Ask user if they want to plot the data
    plot_input = input("\nPlot? (y/n) ")
    if plot_input.lower() == 'y':
        months = [row[0] for row in results]
        ridership = [row[1] for row in results]
        plt.figure(figsize=(10, 5))
        plt.plot(months, ridership)
        plt.title(f"Monthly Ridership at {matching_stations[0][0]} Station ({year})")
        plt.xlabel("Month")
        plt.ylabel("Number of Riders")
        plt.tight_layout()
        plt.show()

# Compares daily ridership between two stations for a given year, displaying the comparison and offering an option to plot the data
def daily_ridership_comparison(dbConn, year):
    dbCursor = dbConn.cursor()

    def fetch_daily_ridership(station_name):
        nonlocal flag
        dbCursor.execute("SELECT Station_ID, Station_Name FROM Stations WHERE Station_Name LIKE ?", ('%' + station_name + '%',))
        matching_stations = dbCursor.fetchall()
        if len(matching_stations) == 0:
            print("**No station found...")
            flag = 1
            return None, None, None
        elif len(matching_stations) > 1:
            print("**Multiple stations found...")
            flag = 1
            return None, None, None
        station_id, exact_station_name = matching_stations[0]

        dbCursor.execute("""
            SELECT strftime('%Y-%m-%d', Ride_Date) as Date, SUM(Num_Riders) as Total_Riders
            FROM Ridership WHERE Station_ID = ? AND strftime('%Y', Ride_Date) = ?
            GROUP BY Date ORDER BY Date
        """, (station_id, year))
        return station_id, exact_station_name, dbCursor.fetchall()

    flag = 0
    station1 = input("\nEnter station 1 (wildcards _ and %): ")
    station1_id, station1_name, station1_data = fetch_daily_ridership(station1)
    if flag == 1: return

    flag = 0
    station2 = input("\nEnter station 2 (wildcards _ and %): ")
    station2_id, station2_name, station2_data = fetch_daily_ridership(station2)
    if flag == 1: return

    print(f"Station 1: {station1_id} {station1_name}")
    for date, riders in station1_data[:5] + station1_data[-5:]:
        print(f"{date} {riders}")

    print(f"Station 2: {station2_id} {station2_name}")
    for date, riders in station2_data[:5] + station2_data[-5:]:
        print(f"{date} {riders}")

    plot_input = input("\nPlot? (y/n) ")
    if plot_input.lower() == 'y':
        # Convert date strings to datetime objects
        dates = [datetime.strptime(row[0], '%Y-%m-%d') for row in station1_data]
        # Convert datetime objects to day of year
        days = [(date - datetime(date.year, 1, 1)).days + 1 for date in dates]

        ridership1 = [row[1] for row in station1_data]
        ridership2 = [row[1] for row in station2_data]

        plt.figure(figsize=(10, 5))
        plt.plot(days, ridership1, label=f"{station1_name}")
        plt.plot(days, ridership2, label=f"{station2_name}")  

        plt.title(f"Ridership Each Day of {year}")
        plt.xlabel("Day")
        plt.ylabel("Number of Riders")

        plt.xticks(range(0, max(days) + 1, 50))  # Set x-axis ticks at intervals of 50
        plt.legend()
        plt.tight_layout()
        plt.show()

# Finds and lists all stations within a mile of a given latitude and longitude, with an option to plot these stations on a map
def stations_within_mile(dbConn, latitude):
    dbCursor = dbConn.cursor()
    
    # Validate latitude and longitude bounds
    if not (40 <= latitude <= 43):
        print("**Latitude entered is out of bounds...")
        return
    
    longitude = float(input("Enter a longitude: "))

    if not (-88 <= longitude <= -87):
        print("**Longitude entered is out of bounds...")
        return

    # Calculate the bounds for a mile without rounding first
    mile_in_degrees_lat = 1 / 69
    mile_in_degrees_lon = 1 / (math.cos(math.radians(latitude)) * 69.17)
    lat_upper = latitude + mile_in_degrees_lat
    lat_lower = latitude - mile_in_degrees_lat
    lon_left = longitude - mile_in_degrees_lon
    lon_right = longitude + mile_in_degrees_lon

    # Adjust the bounds with rounding only for the SQL query
    lat_upper = round(lat_upper + 0.000, 3)
    lat_lower = round(lat_lower - 0.000, 3)
    lon_left = round(lon_left - 0.0001, 3)
    lon_right = round(lon_right + 0.0001, 3)

    # SQL query to select stations within the bounds
    query = """
    SELECT DISTINCT Stations.Station_Name, Stops.Latitude, Stops.Longitude
    FROM Stops
    JOIN Stations ON Stops.Station_ID = Stations.Station_ID
    WHERE Stops.Latitude BETWEEN ? AND ? AND Stops.Longitude BETWEEN ? AND ?
    """
    dbCursor.execute(query, (lat_lower, lat_upper, lon_left, lon_right))

    results = dbCursor.fetchall()

    # Sorting results by station name and rounding coordinates to three decimal places
    sorted_results = sorted(results, key=lambda x: x[0])

    if sorted_results:
        print("\nList of Stations Within a Mile")
        for station_name, lat, lon in sorted_results:
            print(f"{station_name} : ({lat}, {lon})")
        plot_input = input("\nPlot? (y/n) ")
        if plot_input.lower() == 'y':
            # Plot the stations on the provided Chicago map
            image = plt.imread("chicago.png")
            xydims = [-87.9277, -87.5569, 41.7012, 42.0868]  # Map boundaries
            plt.imshow(image, extent=xydims)
            plt.scatter([lon for _, _, lon in results], [lat for _, lat, _ in results], marker='o', color='blue')
            for station_name, lat, lon in results:
                plt.annotate(station_name, (lon, lat))
            plt.title("Stations Near You")
            plt.xlim([-87.9277, -87.5569])
            plt.ylim([41.7012, 42.0868])
            plt.show()
    else:
        print("**No stations found...")

def main():
    dbConn = sqlite3.connect('CTA2_L_daily_ridership.db')
    print_stats(dbConn)
    
    # User command loop
    while True:
        command = input("\nPlease enter a command (1-9, x to exit): ")
        if command.lower() == "x":
            break
        elif command == "1":
            partial_station_name = input("\nEnter partial station name (wildcards _ and %): ")
            find_station_names(dbConn, partial_station_name)
        elif command == "2":
            station_name = input("\nEnter the name of the station you would like to analyze: ")
            ridership_percentages(dbConn, station_name)
        elif command == "3":
            total_weekday_ridership(dbConn)
        elif command == "4":
            line_color = input("\nEnter a line color (e.g. Red or Yellow): ").strip().title()
            list_stops_by_line_and_direction(dbConn, line_color)
        elif command == "5":
            stops_for_each_color_by_direction(dbConn)
        elif command == "6":
            station_name = input("\nEnter a station name (wildcards _ and %): ")
            yearly_ridership(dbConn, station_name)
        elif command == "7":
            station_name = input("\nEnter a station name (wildcards _ and %): ")
            monthly_ridership(dbConn, station_name)
        elif command == "8":
            year = input("\nYear to compare against? ")
            daily_ridership_comparison(dbConn, year)
        elif command == "9":
            lat = float(input("\nEnter a latitude: "))
            stations_within_mile(dbConn, lat)
        else:
            print("**Error, unknown command, try again...")

if __name__ == "__main__":
    main()