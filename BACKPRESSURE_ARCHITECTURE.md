# Backpressure Architecture

## Principio de Diseño

El backpressure está **completamente encapsulado dentro del dominio del sink**, manteniendo la separación de responsabilidades limpia:

```
Flow Layer:     Simple send_batch() calls
     ↓
Sink Layer:     ← Backpressure control happens here
     ↓  
Redis Layer:    Stream depth monitoring
```

## Implementación Correcta

### 1. **Autocontención en el Sink**
```python
class HistoricalRedisSink:
    async def send_batch(self, blocks: list[Block], **kwargs) -> None:
        """
        Send batch with automatic backpressure handling.
        Callers don't need to know about flow control.
        """
        await self._handle_backpressure()  # Internal control
        # ... rest of processing
        
    async def _handle_backpressure(self) -> None:
        """All backpressure logic contained here."""
        if self.backpressure_monitor:
            await self.backpressure_monitor.wait_if_paused()
            
    def is_backpressure_active(self) -> bool:
        """Public method for monitoring if needed."""
        return (
            self.backpressure_monitor is not None 
            and self.backpressure_monitor.is_paused
        )
```

### 2. **Flow Layer Simplificado**
```python
# flows/historical.py - NO backpressure logic here
async for blocks in client.epoch_blocks(epoch):
    for block in blocks:
        batch.append(block)
        if len(batch) >= batch_size:
            await sink.send_batch(batch, epoch=epoch)  # Sink handles backpressure
```

## Características

- ✅ **Separation of Concerns**: Flow layer se enfoca en lógica de negocio
- ✅ **Encapsulation**: Todo el control de flujo está en el sink 
- ✅ **Transparency**: Los llamadores no necesitan conocer detalles de backpressure
- ✅ **Testability**: Se puede testear el backpressure independientemente
- ✅ **Maintainability**: Cambios al backpressure no afectan el flow

## Configuración

El backpressure se configura automáticamente via Redis settings:

```python
# config/settings.py
max_stream_depth: int = 10_000  # Pausa cuando excede este límite
check_interval: int = 5         # Verifica cada N segundos
```

El sink inicializa y gestiona su propio monitor sin requerir configuración externa.