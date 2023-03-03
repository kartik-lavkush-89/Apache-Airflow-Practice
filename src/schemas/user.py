def userEntity(item) -> dict:
    return {
        "id" : str(item["_id"]),
        "name" : item["name"],
        "age" : item["age"]
    }