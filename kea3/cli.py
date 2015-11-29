
import sys

import leip


def dispatch():
    """
    Run the Kea3 app
    """

    # if len(sys.argv) > 1:
        
    #     k3commands = set([x for x in app.leip_commands.keys() if '.' not in x])
    #     k3commands |= set(app.leip_subparsers.keys())
    #     command = sys.argv[1]
    #     if command not in k3commands:
    #         sys.argv = sys.argv[:1] + ['run'] + sys.argv[1:]
            
    app.run()


app = leip.app(name='kea3')
app.discover(globals())
