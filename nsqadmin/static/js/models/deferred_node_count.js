var _ = require('underscore');

var AppState = require('../app_state');
var Backbone = require('backbone');


var DeferredNodeCount = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Topic() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function () {
        return AppState.apiPath('/deferredNodeCount');
    },

    parse: function (response) {
        var totalDepth = 0
        var totalDeliveryRC = 0
        _.map(response['deferred_node_stats'] || [], function (item) {
            totalDepth += item['total_depth']
            totalDeliveryRC += item['delivery_rc']
            return item;
        });
        response['total_depth'] = totalDepth
        response['total_delivery_rc'] = totalDeliveryRC
        return response;
    }
});

module.exports = DeferredNodeCount;
