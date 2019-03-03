const {
  flow,
  filter,
  join,
  map,
  mapValues,
  omit,
  pluck,
} = require('lodash/fp')

const temporalDefaultOptions = {
  // runs the insert within the sequelize hook chain, disable
  // for increased performance
  blocking: true,
  full: false,
}

const Temporal = (model, sequelize, temporalOptions) => {
  temporalOptions = { ...temporalDefaultOptions, ...temporalOptions }

  const Sequelize = sequelize.Sequelize

  const historyName = model.name + 'History'
  //const historyName = model.getTableName() + 'History'
  //const historyName = model.options.name.singular + 'History'

  const historyOwnAttrs = {
    hid: {
      type: Sequelize.BIGINT,
      primaryKey: true,
      autoIncrement: true,
      unique: true,
    },
    archivedAt: {
      type: Sequelize.DATE,
      allowNull: false,
      defaultValue: Sequelize.NOW,
    },
  }

  const excludedAttributes = [
    'Model',
    'unique',
    'primaryKey',
    'autoIncrement',
    'set',
    'get',
    '_modelAttribute'
  ]
  const historyAttributes = {
    ...mapValues(v => {
      v = omit(excludedAttributes)(v)
      // remove the "NOW" defaultValue for the default timestamps
      // we want to save them, but just a copy from our master record
      if (v.fieldName == 'createdAt' || v.fieldName == 'updatedAt') {
        v.type = Sequelize.DATE
      }
      v.allowNull = true
      return v
    })(model.rawAttributes),
    ...historyOwnAttrs,
  }

  const historyOwnOptions = {
    timestamps: false,
  }
  const excludedNames = [
    'name',
    'tableName',
    'sequelize',
    'uniqueKeys',
    'hasPrimaryKey',
    'hooks',
    'scopes',
    'instanceMethods',
    'defaultScope',
  ]
  const modelOptions = omit(excludedNames)(model.options)
  const historyOptions = { ...modelOptions, ...historyOwnOptions }

  // We want to delete indexes that have unique constraint
  const indexes = historyOptions.indexes

  if (Array.isArray(indexes)) {
    historyOptions.indexes = flow(
      filter(idx => !idx.unique && idx.type !== 'UNIQUE'),
      map(idx => ({ ...idx, name: join('_')([historyName, ...idx.fields]) }))
    )(indexes)
  }

  const modelHistory = sequelize.define(historyName, historyAttributes, historyOptions)

  // we already get the updatedAt timestamp from our models
  const insertHook = (obj, options) => {
    const dataValues = (!temporalOptions.full && obj._previousDataValues) || obj.dataValues
    const historyRecord = modelHistory.create(dataValues, { transaction: options.transaction })
    if (temporalOptions.blocking) {
      return historyRecord
    }
  }

  const insertBulkHook = (options) => {
    if (!options.individualHooks) {
      const queryAll = model
        .findAll({ where: options.where, transaction: options.transaction })
        .then((hits) => {
          if (hits) {
            return modelHistory.bulkCreate(pluck('dataValues')(hits), { transaction: options.transaction })
          }
        })
      if (temporalOptions.blocking) {
        return queryAll
      }
    }
  }

  // use `after` to be nonBlocking
  // all hooks just create a copy
  if (temporalOptions.full) {
    model.addHook('afterCreate', insertHook)
    model.addHook('afterUpdate', insertHook)
    model.addHook('afterDestroy', insertHook)
    model.addHook('afterRestore', insertHook)
  } else {
    model.addHook('beforeUpdate', insertHook)
    model.addHook('beforeDestroy', insertHook)
  }

  model.addHook('beforeBulkUpdate', insertBulkHook)
  model.addHook('beforeBulkDestroy', insertBulkHook)

  const readOnlyHook = () => {
    throw new Error("This is a read-only history database. You aren't allowed to modify it.")
  }

  modelHistory.addHook('beforeUpdate', readOnlyHook)
  modelHistory.addHook('beforeDestroy', readOnlyHook)

  return model
}

module.exports = Temporal