class SendError(Exception):
    def __init__(self):
        self.mensaje = 'Error procesing the message'

    def __str__(self):
        return f'{self.mensaje}'
