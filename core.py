from flask import Flask, jsonify, request, abort, make_response
from peewee import IntegrityError, DoesNotExist
from datetime import datetime, timedelta
from redis import Redis
from peewee import MySQLDatabase, Model, IntegerField, DateTimeField, BooleanField

app = Flask(__name__)
db = MySQLDatabase("testapp", host='localhost', user='root', passwd='IePhoz3of4tac3')


class Item(Model):
    id = IntegerField(unique=True, primary_key=True)
    start_time = DateTimeField()
    days = IntegerField()
    end_percent = IntegerField()
    start_price = IntegerField()
    expired = BooleanField()

    class Meta:
        database = db


@app.route('/item/show/<int:item_id>', methods=['GET'])
def item_show(item_id):
    redis_connection = Redis(host='localhost', port=6379, decode_responses=True, encoding='utf-8')
    if redis_connection.exists(item_id):
        result = redis_connection.hgetall(item_id)
    # if value was not found in cache, lookup in mysql
    else:
        try:
            cur_item = Item.get((Item.id == item_id) & (Item.expired == False))
        except DoesNotExist:
            abort(404, 'No item with ID ' + str(item_id))
        else:
            result = {}
            # if item is not expired but time already passed over days+12H
            if datetime.now() > (cur_item.start_time + timedelta(days=cur_item.days, hours=12)):
                cur_item.expired = True
                cur_item.save()
                abort(404, 'No item with ID ' + str(item_id))
            days_passed = (datetime.now() - cur_item.start_time).days
            result['current_price'] = (100 - days_passed * (100 - cur_item.end_percent) / cur_item.days)\
                                      / 100 * cur_item.start_price
            if days_passed == cur_item.days:
                result['is_price_min'] = True
                # keep for 12 hours in redis
                ttl = ((cur_item.start_time + timedelta(days=days_passed, hours=12)) - datetime.now())
                pass
                # mark item in mysql as expired
                cur_item.expired = True
                cur_item.save()
            else:
                result['is_price_min'] = False
                # keep for next day changing in redis
                ttl = ((cur_item.start_time + timedelta(days=days_passed + 1)) - datetime.now())
            # write value to redis with TTL
            redis_connection.hmset(item_id, result)
            redis_connection.expire(item_id, ttl)
    result['id'] = item_id
    return jsonify(result)


@app.route('/item/add', methods=['POST'])
def item_add():
    if not request.json:
        abort(400, 'Input data must be JSON-formatted')
    try:
        if int(request.json['end_percent']) not in range(1, 100):
            abort(400, 'Percent must be from 1 to 100')
        for key in ['id', 'days', 'start_price']:
            if int(request.json[key]) <= 0:
                abort(400, 'Integer values cannot be negative')
        datetime.strptime(request.json['start_time'], '%Y-%m-%d %H:%M:%S.%f')
        Item.create(id=request.json['id'], start_time=request.json['start_time'], days=request.json['days'],
                    end_percent=request.json['end_percent'], start_price=request.json['start_price'], expired=False)
    except (ValueError, KeyError):
        abort(400, 'Validation error')
    except IntegrityError:
        abort(400, 'Duplicate ID')
    return jsonify({'Result': 'Item added'}), 201


@app.errorhandler(400)
def e400(error):
    return make_response(jsonify({'Error': error.description}), 400)


@app.errorhandler(404)
def e404(error):
    return make_response(jsonify({'Error': error.description}), 404)

if __name__ == '__main__':
    db.connect()
    app.run(host='0.0.0.0')
