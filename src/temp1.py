def process_data(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        process_data(item)
            elif isinstance(value, str):
                print(value)
            else:
                continue
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                process_data(item)
            else:
                continue
    else:
        return