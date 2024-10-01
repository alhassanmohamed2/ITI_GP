import json
from datetime import datetime

def Transf_data(value):

    data = json.loads(value)
    transformed_data = {}

    transformed_data['beach_name'] = data.get('Beach Name', '').strip()

    timestamp_str = data.get('Measurement Timestamp', '')
    try:
        transformed_data['measurement_timestamp'] = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f").isoformat()
    except ValueError:
        transformed_data['measurement_timestamp'] = None
        
    try:
        transformed_data['water_temperature_celsius'] = float(data.get('Water Temperature', None))
    except (ValueError, TypeError):
        transformed_data['water_temperature_celsius'] = None

    try:
        transformed_data['turbidity_ntu'] = float(data.get('Turbidity', None))
    except (ValueError, TypeError):
        transformed_data['turbidity_ntu'] = None

    try:
        transformed_data['transducer_depth_meters'] = float(data.get('Transducer Depth', None))
    except (ValueError, TypeError):
        transformed_data['transducer_depth_meters'] = None

    try:
        transformed_data['wave_height_meters'] = float(data.get('Wave Height', None))
    except (ValueError, TypeError):
        transformed_data['wave_height_meters'] = None

    try:
        transformed_data['wave_period_seconds'] = int(data.get('Wave Period', None))
    except (ValueError, TypeError):
        transformed_data['wave_period_seconds'] = None

    try:
        transformed_data['battery_life_voltage'] = float(data.get('Battery Life', None))
    except (ValueError, TypeError):
        transformed_data['battery_life_voltage'] = None

    transformed_data['measurement_id'] = data.get('Measurement ID', '').strip()
    if transformed_data['battery_life_voltage'] and transformed_data['battery_life_voltage'] < 12:
        transformed_data['battery_status'] = 'low'
    else:
        transformed_data['battery_status'] = 'normal'
    return transformed_data
