

class DummyKafkaProducer:
    def send(self, *args, **kwargs):
        print(f"""
                send been used on a dummy Producer not the real one plz verify that a real producer has been created and initiated
                the call has been done on {self} 
            """
              )
        exit(1)
    
    def flush(self):
        pass
    def close(self):
        pass