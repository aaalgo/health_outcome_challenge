from collections import defaultdict

track_layers_context = []

class track_layers:
    def __init__(self, model):
        self.model = model 
        self.counting = None
        self.offset = 0
        seq = getattr(model, "track_sequence", None)
        if seq is None:
            setattr(model, "track_sequence", [])
            self.offset = None
            self.counting = defaultdict(lambda: 0)
            pass
        pass

    def __enter__(self):
        track_layers_context.append(self)
        pass

    def __exit__(self, type, value, traceback):
        track_layers_context.pop()
        pass
    pass

def L (cls, *kargs, **kwargs):
    ctx = track_layers_context[-1]
    if not ctx.offset is None:
        l = ctx.model.track_sequence[ctx.offset]
        assert isinstance(l, cls)
        ctx.offset += 1
        return l
    name = '%s_%d' % (cls.__name__, ctx.counting[cls])
    ctx.counting[cls] += 1
    l = cls(*kargs, **kwargs)
    setattr(ctx.model, name, l)
    ctx.model.track_sequence.append(l)
    return l
