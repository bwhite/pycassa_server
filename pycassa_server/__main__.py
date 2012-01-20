from gevent import monkey
monkey.patch_all()
import os
import argparse
import pycassa
import bottle



@bottle.route('/data.js')
def index():
    return dict(CF.get_range())

def _lists2d_to_html(lists):
    out = []
    for n, l in enumerate(lists):
        if n == 0:
            cur_row = ''.join('<th scope="col">%s</th>' % x for x in l)
        else:
            cur_row = ''.join('<td>%s</td>' % x for x in l)
        if n % 2 != 0:
            cur_row = '<tr class="odd">%s</tr>' % cur_row
        else:
            cur_row = '<tr>%s</tr>' % cur_row
        out.append(cur_row)
    cur_table = ''.join(out)
    return '<table>%s</table>' % cur_table

@bottle.route('/styles.css')
def get_style():
    static_path = os.path.dirname(__file__)
    return bottle.static_file('styles.css', root=static_path)


@bottle.route('/')
def index():
    data = list(CF.get_range())
    column_names = set()
    for key_name, columns in data:
        column_names.update(columns)
    column_names = sorted(map(str, column_names))
    out_rows = [['row_key'] + column_names]
    for key_name, columns in data:
        out_rows.append([key_name] + [columns.get(x, '') for x in column_names])
    templ = os.path.join(os.path.dirname(__file__), 'index')
    return bottle.template(templ, table=_lists2d_to_html(out_rows))

def main():
    global ARGS, CF
    parser = argparse.ArgumentParser(description="Serve a cassandra column family")
    # Server port
    parser.add_argument('keyspace', help='Cassandra keyspace')
    parser.add_argument('column_family', help='Cassandra column family')
    parser.add_argument('--port', help='run webpy on this port', default='8080')
    ARGS = parser.parse_args()
    pool = pycassa.ConnectionPool(ARGS.keyspace)
    CF = pycassa.ColumnFamily(pool, ARGS.column_family)
    bottle.run(host='0.0.0.0', port=ARGS.port, server='gevent', reloader=True)


if __name__ == "__main__":
    main()
