#!/bin/bash

echo "###### Waiting for mongodb1 instance startup.."
until mongosh --host mongodb1:27017 --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 2)' &>/dev/null; do
  printf '.'
  sleep 1
done
echo "###### Working mongodb1 instance found, initiating user setup & initializing rs setup.."

echo "###### Waiting for mongodb2 instance startup.."
until mongosh --host mongodb2:27017 --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 2)' &>/dev/null; do
  printf '.'
  sleep 1
done
echo "###### Working mongodb2 instance found, initiating user setup & initializing rs setup.."


echo "###### Waiting for mongodb3 instance startup.."
until mongosh --host mongodb3:27017 --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 2)' &>/dev/null; do
  printf '.'
  sleep 1
done
echo "###### Working mongodb3 instance found, initiating user setup & initializing rs setup.."

# authenticate and initiate replica set
mongosh --host mongodb1:27017 <<EOF
admin = db.getSiblingDB('admin');
admin.auth('user', 'pass');
admin.createUser(
  {
    user: "konfig",
    pwd: "konfig",
    roles: [{role: "readWrite", db: "konfig"}]
  }
);
EOF

mongosh --host mongodb2:27017 <<EOF
admin = db.getSiblingDB('admin');
admin.auth('user', 'pass');
admin.createUser(
  {
    user: "konfig",
    pwd: "konfig",
    roles: [{role: "readWrite", db: "konfig"}]
  }
);
EOF

mongosh --host mongodb3:27017 <<EOF
admin = db.getSiblingDB('admin');
admin.auth('user', 'pass');
admin.createUser(
  {
    user: "konfig",
    pwd: "konfig",
    roles: [{role: "readWrite", db: "konfig"}]
  }
);
rs.initiate(
  {
      _id: "rs",
      members: [
          {
              _id: 1,
              host: "mongodb1:27017",
          },
          {
              _id: 2,
              host: "mongodb2:27017",
          },
          {
              _id: 3,
              host: "mongodb3:27017",
          }
      ]
  }, {
    force: true
  }
);
rs.status();
EOF

mongosh -u user -p pass --host rs/mongodb1,mongodb2,mongodb3:27017 <<EOF
rs.status();
EOF