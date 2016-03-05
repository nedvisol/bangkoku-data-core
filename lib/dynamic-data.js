'use strict';

/****

Backend data store requirements:

1. Data Table
 - Fields:
    _id
    _created
    _rev
    _class
    _txn

Hash Key = _id
Sort Key = _created


_id = [id]@[partition-id]/i  (item)
_id = [id]@[partition-id]/[relationship-id]/r  (relationship)

DD.getContext('partition-id') => Q(ctx)


ctx.get('id') => Q(json)
ctx.for('class').create(json) => Q(id)
ctx.update(json) => Q(json)
ctx.delete('id') => Q(true)
ctx.for('relationship').getRelatedTo('id')
ctx.for('relationship').relate('id','id')

ctx.newTxn().expiresIn(3000) => ctx
ctx.commit()
ctx.abort()




***/
