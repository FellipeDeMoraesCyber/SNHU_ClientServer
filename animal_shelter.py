from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """
    def __init__(self):
        pass
    
    def __init__(self, user, passwd, host, port, db, col):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # You must edit the connection variables below to reflect
        # your own instance of MongoDB!
        #
        # Connection Variables
        #
        USER = user
        PASS = passwd
        HOST = host
        PORT = port
        DB = db
        COL = col
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

    def is_valid_date(self, date_string):
      """
      Checks if a date string in 'MM-DD-YYYY' format represents a valid date.

      Args:
        date_string: The date string to check.

      Returns:
        True if the date is valid, False otherwise.
      """
      try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
      except ValueError:
        return False

# Complete this create method to implement the C in CRUD.
    def create(self, data):
        """
        Inserts a document into the specified MongoDB collection.

        Args:
            data (dict): A dictionary representing the document to be inserted.

        Returns:
            bool: True if the insert is successful, False otherwise.
            Exception: raised if no parameter is given
        """
        if data is not None:
            try: 
                if not isinstance(data["animal_id"], str):
                    raise Exception("animal_id data type is not string. animal_id accepts strictly string data type.")
                elif data["animal_id"] == "":
                    raise Exception("animal_id is empty, it is required to have a value.")
                elif data["name"] == "":
                    raise Exception("name is empty error")
                elif not self.is_valid_date(data["date_of_birth"]): 
                    raise Exception("date needs to be YYYY-MM-DD with valid numbers")
                result = self.database.animals.insert_one(data)  # data should be dictionary         
                return result.acknowledged
            except Exception as e:
                print(f"Error inserting document: {e}")
                return False
        else:
            raise Exception("Nothing to save, because data parameter is empty")

# Create method to implement the R in CRUD.
    def read(self, query={}, water=False, wild=False, disaster=False):

        """
        Queries for documents from the specified MongoDB collection.

        Args:
            query (dict): A dictionary representing the query to be used with the find() method.

        Returns:
            list: A list of documents matching the query, or an empty list if an error occurs or no match is found.
        """
        if self.collection is None:
            return []
        if water:
            query = {
                "$or": [
                    {"breed": {"$regex": "Labrador Retriever"}},
                    {"breed": {"$regex": "Chesapeake Bay Retriever"}},
                    {"breed": {"$regex": "Newfoundland"}}
                ],
                "sex_upon_outcome": "Intact Female",
                "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156}
            }
        elif wild:
            query = {
               "$or": [
                    {"breed": {"$regex": "German Shepherd"}},
                    {"breed": {"$regex": "Alaskan Malamute"}},
                    {"breed": {"$regex": "Old English Sheepdog"}},
                    {"breed": {"$regex": "Siberian Husky"}},
                    {"breed": {"$regex": "Rottweiler"}}
                ],
                "sex_upon_outcome": "Intact Male",
                "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156}
           }
        elif disaster:
            query = {
               "$or": [
                    {"breed": {"$regex": "Doberman Pinscher"}},
                    {"breed": {"$regex": "German Shepherd"}},
                    {"breed": {"$regex": "Golden Retriever"}},
                    {"breed": {"$regex": "Bloodhound"}},
                    {"breed": {"$regex": "Rottweiler"}}
                ],
                "sex_upon_outcome": "Intact Male",
                "age_upon_outcome_in_weeks": {"$gte": 20, "$lte": 300} 
            }
            
        try:
            results = list(self.collection.find(query))
            return results
        except Exception as e:
            print(f"Error querying documents: {e}")
            return []

# Update 
    def update(self, query, update_data, many=False):
        """
        Updates documents in the MongoDB collection based on query
        
        Args: 
            Query (dict): the query to filter documents
            update_data (dict): The update operation to apply
            many (bool, optional): If True, updates multiple documents; otherwise updates one. Defaults to False.
        """
        try:
            if many:
                result = self.collection.update_many(query, {"$set": update_data})
                return result.modified_count
            else:
                result = self.collection.update_one(query, {"$set": update_data})
                return result.modified_count
        except Exception as e:
            print(f"An error occured: {e}")
            return 0
        
# Delete
    def delete(self, query, many=False):
        """
        Deletes documents from the MongoDB collection based on a query.

        Args:
            query (dict): The query to filter documents.
            many (bool, optional): If True, deletes multiple documents; otherwise, deletes one. Defaults to False.

        Returns:
            int: The number of documents deleted.
        """
        try:
            if many:
                result = self.collection.delete_many(query)
                return result.deleted_count
            else:
                result = self.collection.delete_one(query)
                return result.deleted_count

        except Exception as e:
            print(f"An error occurred: {e}")
            return 0  # Or raise the exception, depending on your error handling policy


        
        
# close DB connection
    def close(self):
        self.client.close()
        

        
    
