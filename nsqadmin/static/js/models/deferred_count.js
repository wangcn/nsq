var _ = require('underscore');

var AppState = require('../app_state');
var Backbone = require('backbone');

var DeferredCount = Backbone.Model.extend({
    idAttribute: 'name',

    constructor: function Topic() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    url: function () {
        var node = this.get('name')
        if (node != null) {
            return AppState.apiPath('/deferredCount?node=' + encodeURIComponent(node));
        }
        return AppState.apiPath('/deferredCount');
    },

    parse: function (response) {
        var total = 0
        _.map(response['deferred_depth'] || [], function (item) {
            total += item['depth']
            return item;
        });
        response['total'] = total
        return response;
    }
});

module.exports = DeferredCount;
