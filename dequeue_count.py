import os
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
from azure.core.exceptions import ResourceExistsError
from sample_queue import AzureQueueManager

class AzureQueueManagerExtended(AzureQueueManager):
    """
    Extensión de AzureQueueManager con funcionalidades para consultar mensajes recibidos previamente
    """
    
    def receive_and_track_messages(self, max_messages=5):
        """
        Recibir mensajes de la cola y verificar cuáles han sido recibidos previamente
        
        :param max_messages: Número máximo de mensajes a recibir (1-32)
        :return: Tupla con (mensajes_nuevos, mensajes_recibidos_previamente)
        """
        try:
            print(f"Recibiendo hasta {max_messages} mensajes...")
            messages = self.queue_client.receive_messages(max_messages=max_messages)
            
            new_messages = []
            previously_received = []
            
            for message in messages:
                # El dequeue_count indica cuántas veces se ha recibido el mensaje
                # Si es mayor que 1, significa que ya se recibió anteriormente
                if message.dequeue_count > 1:
                    print(f"Mensaje recibido previamente: {message.content} (ID: {message.id}, "
                          f"Dequeue Count: {message.dequeue_count})")
                    previously_received.append(message)
                else:
                    print(f"Mensaje nuevo: {message.content} (ID: {message.id})")
                    new_messages.append(message)
            
            if not new_messages and not previously_received:
                print("No hay mensajes en la cola")
            
            return new_messages, previously_received
        except Exception as e:
            print(f"Error al recibir y rastrear mensajes: {str(e)}")
            return [], []
    
    def get_message_details(self, message):
        """
        Obtener detalles completos de un mensaje
    
        :param message: Objeto mensaje
        :return: Diccionario con los detalles del mensaje
        """
    # Crear un diccionario base con los atributos garantizados
        details = {
            "id": message.id,
            "content": message.content,
            "dequeue_count": message.dequeue_count,
            "pop_receipt": message.pop_receipt
        }
    
        # Agregar atributos opcionales que podrían estar disponibles en algunas versiones
        # Usar getattr con un valor predeterminado para evitar errores
        for attr in ["insertion_time", "expiration_time", "time_next_visible"]:
            if hasattr(message, attr):
                details[attr] = getattr(message, attr)
    
        return details
    
    def process_messages_with_tracking(self, max_messages=5, auto_delete_processed=False):
        """
        Procesar mensajes con seguimiento de estado
        
        :param max_messages: Número máximo de mensajes a procesar
        :param auto_delete_processed: Si True, elimina automáticamente los mensajes procesados
        :return: Diccionario con resumen de procesamiento
        """
        new_messages, previously_received = self.receive_and_track_messages(max_messages)
        
        processing_summary = {
            "new_messages_count": len(new_messages),
            "previously_received_count": len(previously_received),
            "new_messages": [],
            "previously_received": []
        }
        
        # Procesar mensajes nuevos
        for msg in new_messages:
            print(f"Procesando mensaje nuevo: {msg.content}")
            # Aquí iría la lógica para procesar el mensaje
            processing_summary["new_messages"].append(self.get_message_details(msg))
            
            if auto_delete_processed:
                self.delete_message(msg)
        
        # Procesar mensajes recibidos previamente (posible reprocesamiento)
        for msg in previously_received:
            print(f"Verificando mensaje recibido previamente: {msg.content}")
            # Aquí iría la lógica para manejar mensajes duplicados
            processing_summary["previously_received"].append(self.get_message_details(msg))
            
            if auto_delete_processed:
                self.delete_message(msg)
        
        return processing_summary

# Ejemplo de uso
if __name__ == "__main__":
    connection_string = "Hello"
    
    queue_manager = AzureQueueManagerExtended(connection_string, queue_name)
    
    # Verificar si la cola existe y crearla si no
    #queue_manager.create_queue()
    
    # Ejemplo: Enviar algunos mensajes
    #queue_manager.send_message("Mensaje de prueba 1 ")
    #queue_manager.send_message("Mensaje de prueba 2")
    
    # Recibir y procesar mensajes con tracking
    processing_results = queue_manager.process_messages_with_tracking(max_messages=10)
    
    print("\nResumen de procesamiento:")
    print(f"Mensajes nuevos: {processing_results['new_messages_count']}")
    print(f"Mensajes recibidos previamente: {processing_results['previously_received_count']}")
    
    # Puedes recibir mensajes nuevamente para ver el dequeue_count incrementado
    print("\nRecibiendo mensajes una segunda vez:")
    processing_results = queue_manager.process_messages_with_tracking(max_messages=10, auto_delete_processed=True)