from datetime import datetime, timedelta, timezone

# Start date in UTC
start_date = datetime(2023, 4, 19, tzinfo=timezone.utc)

# Initialize the epoch, date, and timestamp values
epoch = 0
data_series = []

# Generate data for multiple weeks
for week in range(500):  # Adjust the number of weeks as needed
    # Calculate the date and timestamp for the current week
    current_date = start_date + timedelta(weeks=week)
    timestamp = int(current_date.timestamp())

    # Add the data for the current week to the series
    data_series.append((epoch, current_date.strftime('%d-%m-%Y'), timestamp))

    # Increment the epoch value by 1 for the next week
    epoch += 1

# Print the generated data series
for data_point in data_series:
    print(f"{data_point[0]},{data_point[1]},{data_point[2]}")
