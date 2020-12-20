"""Module containing base Destination Model class"""

from typing import Dict, Any, List


class DestinationBaseModel:

    @classmethod
    def get_attributes(cls) -> List[str]:
        """Returns the attributes/fields this model has"""
        raise NotImplementedError('get_attributes method not implemented.')

    @classmethod
    def get_last_saved_timestamp(cls):
        """Returns the last saved timestamp"""
        return None

    def update(self, *args, **kwargs):
        """
        Updates the attributes passed in the kwargs and saves
        """
        raise NotImplementedError('update method not implemented.')

    def save(self, *args, **kwargs):
        """Commits changes to the store"""
        raise NotImplementedError('save method not implemented.')

    def delete(self, *args, **kwargs):
        """Deletes the current instance"""
        raise NotImplementedError('delete method not implemented.')

    @classmethod
    def initialize(cls):
        """Initializes the model in the store e.g. creating tables in database"""
        raise NotImplementedError('initialize class method not implemented.')

    @classmethod
    def upsert(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        """
        Updates the given instance or creates it if it does not exist
        Returns data so that it can be used by the next pipe
        """
        raise NotImplementedError('upsert method not implemented.')
