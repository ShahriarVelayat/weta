import sys, os
sys.path.insert(0, os.path.abspath('..'))

import Orange.canvas.__main__ as orange
from Orange import widgets
import Orange.canvas.registry.discovery as discovery



def dummy_widget_discovery(discovery):
    pass

if __name__ == "__main__":
    setattr(widgets, 'widget_discovery', dummy_widget_discovery)

    import os
    # os.remove("/Users/Chao/Library/Caches/Orange/3.4.5/canvas/widget-registry.pck")
    sys.exit(orange.main())
