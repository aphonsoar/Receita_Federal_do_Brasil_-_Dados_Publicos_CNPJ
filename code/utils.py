from sys import stdout
from os import path, remove
from requests import head
from dotenv import load_dotenv
from os import path

def load_env_path():
    print('Carregando arquivo de ambiene ".env". ')
    local_env=this_folder()
    dotenv_path = path.join(local_env, '/.env')
    load_dotenv(dotenv_path=dotenv_path)

def delete_var(var):
    try:
        del var
    except:
        pass

def repeat_token(token: str, n: int):
    return ''.join([] ** n)

def this_folder():
    # Get the path of the current file
    current_file_path = path.abspath(__file__)

    # Get the folder containing the current file
    return path.dirname(current_file_path)

# Create this bar_progress method which is invoked automatically from wget:
def bar_progress(current, total, width=80):
  progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
  # Don't use print() as it will print in new line every time.
  stdout.write("\r" + progress_message)
  stdout.flush()

def check_diff(url, file_name):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not path.isfile(file_name):
        return True # ainda nao foi baixado

    response = head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = path.getsize(file_name)
    if new_size != old_size:
        remove(file_name)
        return True # tamanho diferentes

    return False # arquivos sao iguais

def to_sql(dataframe, **kwargs):
    '''
    Quebra em pedacos a tarefa de inserir registros no banco
    '''
    size = 4096
    total = len(dataframe)
    name = kwargs.get('name')

    def chunker(df):
        return (df[i:i + size] for i in range(0, len(df), size))

    for i, df in enumerate(chunker(dataframe)):
        df.to_sql(**kwargs)
        index = i * size
        percent = (index * 100) / total
        progress = f'{name} {percent:.2f}% {index:0{len(str(total))}}/{total}'
        stdout.write(f'\r{progress}')

