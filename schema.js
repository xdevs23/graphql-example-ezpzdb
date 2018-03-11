const {
    GraphQLObjectType,
    GraphQLString,
    GraphQLInt,
    GraphQLSchema,
    GraphQLList,
    GraphQLBoolean,
    GraphQLNonNull
} = require('graphql')

const db = require('./ezpzdb').db()

// Customer Type
const CustomerType = new GraphQLObjectType({
    name: 'Customer',
    fields:() => ({
        id: { type: GraphQLInt },
        firstname: { type: GraphQLString },
        name: { type: GraphQLString },
        street: { type: GraphQLString },
        city: { type: GraphQLString }
    })
})

// Root Query
const RootQuery = new GraphQLObjectType({
    name: 'RootQueryType',
    fields: {
        customer: {
            type: CustomerType,
            args: {
                id: { type: GraphQLInt }
            },
            resolve (parentValue, args) {
                return db.get('customers', args.id)
            }
        },
        customers: {
            type: new GraphQLList(CustomerType),
            resolve (parentValue, args) {
                return db.getAll('customers')
            }
        }
    }
})

// Mutations
const mutation = new GraphQLObjectType({
    name: 'Mutation',
    fields: {
        addCustomer: {
            type: CustomerType,
            args: {
                firstname: { type: new GraphQLNonNull(GraphQLString) },
                name: { type: new GraphQLNonNull(GraphQLString) },
                email: { type: new GraphQLNonNull(GraphQLString) },
                street: { type: new GraphQLNonNull(GraphQLString) },
                city: { type: new GraphQLNonNull(GraphQLString) }
            },
            resolve (parentValue, args) {
              return { id: db.insert('customers', args) }
            }
        },
        deleteCustomer: {
            type: GraphQLBoolean,
            args: {
                id: { type: new GraphQLNonNull(GraphQLInt) }
            },
            resolve (parentValue, args) {
              return db.remove('customers', args.id)
            }
        },
        editCustomer: {
            type: CustomerType,
            args: {
                id: { type: new GraphQLNonNull(GraphQLInt) },
                firstname: { type: GraphQLString },
                name: { type: GraphQLString },
                email: { type: GraphQLString },
                street: { type: GraphQLString },
                city: { type: GraphQLString }
            },
            resolve (parentValue, args) {
              return db.update('customers', args)
            }
        },
        deleteAllCustomers: {
          type: GraphQLInt,
          resolve (parentValue, args) {
            return db.truncate('customers')
          }
        },
        benchmarkCustomer: {
          type: GraphQLBoolean,
          args: {
            amount: { type: new GraphQLNonNull(GraphQLInt) }
          },
          resolve (parentValue, args) {
            let amount = args.amount
            console.log("Doing benchmark...")
            global.gc()
            for (let i = 1; i <= amount; i++) {
              console.log(`${i}`)
              db.insert('customers', {
                name: `Name ${i}`,
                firstname: `Firstname ${i}`,
                email: "example@mail.com",
                street: `${i} South Street`,
                city: `Los Angeles`
              })
              if (i % 1000000 === 0) {
                global.gc()
              }
            }
            return true
          }
        },
        evalJs: {
          type: GraphQLString,
          args: {
            js: { type: new GraphQLNonNull(GraphQLString) }
          },
          resolve (parentValue, args) {
            return eval(args.js)
          }
        }
    }
})

module.exports = new GraphQLSchema({
    query: RootQuery,
    mutation
})
