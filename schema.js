const axios = require('axios')
const {
    GraphQLObjectType,
    GraphQLString,
    GraphQLInt,
    GraphQLSchema,
    GraphQLList,
    GraphQLNonNull
} = require('graphql')

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
                return axios.get('http://localhost:3000/customers/' + args.id)
                    .then(res => res.data)
            }
        },
        customers: {
            type: new GraphQLList(CustomerType),
            resolve (parentValue, args) {
                return axios.get('http://localhost:3000/customers')
                    .then(res => res.data)
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
                return axios.post('http://localhost:3000/customers', args)
                  .then(res => res.data)
            }
        },
        deleteCustomer: {
            type: CustomerType,
            args: {
                id: { type: new GraphQLNonNull(GraphQLInt) }
            },
            resolve (parentValue, args) {
                return axios.delete(
                  `http://localhost:3000/customers/${args.id}`)
                  .then(res => res.data);
            }
        },
        editCustomer: {
            type: CustomerType,
            args: {
                id: { type: new GraphQLNonNull(GraphQLInt) },
                firstname: { type: GraphQLString },
                name: { type: GraphQLString },
                street: { type: GraphQLString },
                city: { type: GraphQLString }
            },
            resolve (parentValue, args) {
                return axios.patch(
                  `http://localhost:3000/customers/${args.id}`, args)
                  .then(res => res.data);
            }
        },
    }
})

module.exports = new GraphQLSchema({
    query: RootQuery,
    mutation
})
