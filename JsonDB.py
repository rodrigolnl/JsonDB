import json
import os
import sys
import time
import warnings
if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl


class Identity:
    pass


class JsonDB:
    def __init__(self, repository: str, path: str = None):
        base_path = str(os.path.dirname(os.path.abspath(__file__))) + '\\repo'
        if not path:
            try:
                os.mkdir(base_path)
            except FileExistsError:
                pass

        self.__Path = f'{path if path else base_path}\\{repository}'
        self.__Collection = None
        self.__Repository = repository
        self.__Data: dict[str | int | float] = {}
        self.__Config: dict[str, dict[str, list | str]] = {}
        self.__load_config()
        self.__IdentityValue = None

        self.__DeletedCollections = []

        if repository not in self.__Config['Repositories']:
            self.__Config['Repositories'][repository] = self.__Path
            self.__save_config()
        else:
            self.__Path = self.__Config['Repositories'][repository]

        try:
            os.mkdir(self.__Path)
        except FileExistsError:
            pass

    def __getitem__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_collection_selected()
        key = str(key)
        try:
            return self.__Data[key]
        except KeyError:
            self.__Data[key] = {}
            return self.__Data[key]
        except TypeError:
            raise KeyError(f'Invalid Key: The key "{key}" is not a valid key in the "{self.__Collection}" collection') from None

    def __setitem__(self, key: str | int | float | type[Identity], value):
        if key is not Identity and type(key) not in [str, int, float]:
            raise KeyError(f'Wrong type: The "{key}" type is not valid, should be of type [int | float | str | Identity]')

        if key is Identity or key.upper() == '_IDENTITY':
            try:
                self.__IdentityValue = int(self.__IdentityValue)
            except ValueError:
                self.__IdentityValue = 1
            while str(self.__IdentityValue) in self.__Data:
                self.__IdentityValue += 1
            key = self.__IdentityValue

        self.__validate_key_type(key)
        self.__validate_collection_selected()
        key = str(key)
        try:
            self.__Data[key] = value
        except KeyError:
            raise KeyError(f'Invalid Key: The key "{key}" is not a valid key in the "{self.__Collection}" collection') from None
        except TypeError:
            raise KeyError(f'Invalid Key: The key "{key}" is not a valid key in the "{self.__Collection}" collection') from None

    def __delitem__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_collection_selected()
        key = str(key)
        try:
            del self.__Data[key]
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in the "{self.__Collection}" collection') from None

    def __iter__(self):
        self.__validate_collection_selected()
        for key in self.__Data:
            yield key

    def __contains__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_collection_selected()
        key = str(key)
        return key in self.__Data

    def __len__(self):
        self.__validate_collection_selected()
        return len(self.__Data)

    def collection(self, name: str):
        if name in self.__DeletedCollections:
            raise KeyError(f'Deleted collection: The collection is marked to be deleted.')
        if type(name) is not str:
            raise TypeError(f'Wrong type: The table\'s name needs to be a string')
        if name == '':
            raise ValueError(f'Wrong value: The table\'s name can not be an empty string')
        self.__Collection = name
        self.__Data = self.__load__(f'{self.__Path}/{self.__Collection}.json')
        if name not in self.__Config['Collections'][self.__Repository]:
            self.__Config['Collections'][self.__Repository].append(name)

        keys = list(self.__Data.keys())
        self.__IdentityValue = keys[-1] if keys else 1
        return self

    def delete(self, collection: str = None):
        self.__DeletedCollections.append(collection)

    def pop(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_collection_selected()
        key = str(key)
        try:
            return self.__Data.pop(key)
        except KeyError:
            raise KeyError(f'Invalid Key: The key "{key}" is not a valid key in "{self.__Collection}" database') from None
        except TypeError:
            raise KeyError(f'Invalid Key: The key "{key}" is not a valid key in "{self.__Collection}" database') from None

    def commit(self):
        for collection in self.__DeletedCollections:
            self.__Config['Collections'][self.__Repository].remove(collection)
            if os.path.exists(f'{self.__Path}/{collection}.json'):
                os.remove(f'{self.__Path}/{collection}.json')
        self.__save__(f'{self.__Path}/{self.__Collection}.json', self.__Data, separators=(',', ':'))
        self.__save_config()

    @property
    def collections(self):
        return self.__Config['Collections'][self.__Repository]

    @property
    def repositories(self):
        return list(self.__Config['Repositories'].keys())

    @property
    def values(self):
        self.__validate_collection_selected()
        return list(self.__Data.values())

    @property
    def keys(self):
        self.__validate_collection_selected()
        return list(self.__Data.keys())

    def __validate_collection_selected(self):
        if self.__Collection is None:
            raise NameError('Table not found: No database selected, use "database(name: string)" to select one')

    def __load_config(self):
        config = self.__load__('config.json')
        config = {} if config is None else config
        edited = False
        if 'Collections' not in config:
            edited = True
            config['Collections'] = {}
            if self.__Repository not in config['Collections']:
                edited = True
                config['Collections'][self.__Repository] = []
        if 'Repositories' not in config:
            edited = True
            config['Repositories'] = {}

        if edited:
            self.__save_config()
        self.__Config = config

    def __save_config(self):
        self.__save__('config.json', self.__Config, 4)

    @staticmethod
    def __custom_formatwarning(msg, *args, **kwargs):
        return str(msg) + '\n'

    @staticmethod
    def __validate_key_type(key):
        if type(key) not in [str, int, float]:
            raise TypeError(f'Wrong type: The key "{key}" needs to be a string, float or integer')

    @staticmethod
    def __save__(path: str, data, indent: int | None = None, separators: tuple[str, str] | None = None):
        finished = False
        file_locked = open(path, 'w')
        file_locked.seek(0)
        while not finished:
            try:
                if sys.platform == 'win32':
                    msvcrt.locking(file_locked.fileno(), msvcrt.LK_LOCK, 1)
                else:
                    fcntl.flock(file_locked.fileno(), fcntl.LOCK_EX)

                json.dump(data, file_locked, indent=indent, separators=separators)
                file_locked.seek(0)

                if sys.platform == 'win32':
                    msvcrt.locking(file_locked.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(file_locked.fileno(), fcntl.LOCK_UN)

                finished = True
            except IOError:
                time.sleep(0.1)
            except ValueError:
                time.sleep(0.1)
            finally:
                file_locked.close()

    @staticmethod
    def __load__(path: str):
        try:
            with open(path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            open(path, 'w+').close()
            return {}
        except json.decoder.JSONDecodeError:
            path = path.replace('/', '\\')
            file = path.split('\\')[-1]
            warnings.formatwarning = JsonDB.__custom_formatwarning
            warnings.warn(f'The file "{file}" could not be decoded or is empty, it will be recreated!')
            return {}
