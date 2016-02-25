(function () {
  var AdminStatusCtrl = function ($scope, Events, $http, $timeout) {
    Events.record(currentUser, "view", "page", "admin/status");
    $scope.$parent.pageTitle = "System Status";

    var refresh = function () {
      $scope.refresh_time = moment().add('minutes', 1);
      $http.get('/status.json').success(function (data) {
        $scope.workers = data.workers;
        delete data.workers;
        $scope.manager = data.manager;
        delete data.manager;
        $scope.status = data;
      });

      $timeout(refresh, 59 * 1000);
    };

    refresh();
  };

  var dateFormatter = function (value) {
    if (!value) {
      return "-";
    }

    return moment(value).format(clientConfig.dateTimeFormat);
  };

  var timestampFormatter = function(value) {
    if (value) {
      return dateFormatter(value * 1000.0);
    }

    return "-";
  }

  var AdminTasksCtrl = function ($scope, Events, $http, $timeout, $filter) {
    Events.record(currentUser, "view", "page", "admin/tasks");
    $scope.$parent.pageTitle = "Running Queries";

    $scope.gridConfig = {
      isPaginationEnabled: true,
      itemsByPage: 50,
      maxSize: 8,
    };
    $scope.currentTab = 'in_progress';
    $scope.tasks = {
      'pending': [],
      'in_progress': [],
      'done': []
    };

    $scope.gridColumns = [
      {
        label: 'Data Source ID',
        map: 'data_source_id'
      },
      {
        label: 'Username',
        map: 'username'
      },
      {
        'label': 'State',
        'map': 'state',
      },
      {
        "label": "Query ID",
        "map": "query_id"
      },
      {
        label: 'Query Hash',
        map: 'query_hash'
      },
      {
        'label': 'Runtime',
        'map': 'run_time',
        'formatFunction': function (value) {
          return $filter('durationHumanize')(value);
        }
      },
      {
        'label': 'Created At',
        'map': 'created_at',
        'formatFunction': timestampFormatter
      },
      {
        'label': 'Started At',
        'map': 'started_at',
        'formatFunction': timestampFormatter
      },
      {
        'label': 'Updated At',
        'map': 'updated_at',
        'formatFunction': timestampFormatter
      }
    ];

    $scope.setTab = function(tab) {
      $scope.currentTab = tab;
      $scope.showingTasks = $scope.tasks[tab];
    };

    var refresh = function () {
      $scope.refresh_time = moment().add('minutes', 1);
      $http.get('/api/admin/queries/tasks').success(function (data) {
        $scope.tasks = data;
        $scope.showingTasks = $scope.tasks[$scope.currentTab];
      });

      $timeout(refresh, 59 * 1000);
    };

    refresh();
  };

  var AdminOutdatedQueriesCtrl = function ($scope, Events, $http, $timeout, $filter) {
    Events.record(currentUser, "view", "page", "admin/outdated_queries");
    $scope.$parent.pageTitle = "Outdated Queries";

    $scope.gridConfig = {
      isPaginationEnabled: true,
      itemsByPage: 50,
      maxSize: 8,
    };

    $scope.gridColumns = [
      {
        label: 'Data Source ID',
        map: 'data_source_id'
      },
      {
        "label": "Name",
        "map": "name",
        "cellTemplateUrl": "/views/queries_query_name_cell.html"
      },
      {
        'label': 'Created By',
        'map': 'user.name'
      },
      {
        'label': 'Runtime',
        'map': 'runtime',
        'formatFunction': function (value) {
          return $filter('durationHumanize')(value);
        }
      },
      {
        'label': 'Last Executed At',
        'map': 'retrieved_at',
        'formatFunction': dateFormatter
      },
      {
        'label': 'Created At',
        'map': 'created_at',
        'formatFunction': dateFormatter
      },
      {
        'label': 'Update Schedule',
        'map': 'schedule',
        'formatFunction': function (value) {
          return $filter('scheduleHumanize')(value);
        }
      }
    ];

    var refresh = function () {
      $scope.refresh_time = moment().add('minutes', 1);
      $http.get('/api/admin/queries/outdated').success(function (data) {
        $scope.queries = data.queries;
        $scope.updatedAt = data.updated_at * 1000.0;
      });

      $timeout(refresh, 59 * 1000);
    };

    refresh();
  };

  angular.module('redash.admin_controllers', [])
         .controller('AdminStatusCtrl', ['$scope', 'Events', '$http', '$timeout', AdminStatusCtrl])
         .controller('AdminTasksCtrl', ['$scope', 'Events', '$http', '$timeout', '$filter', AdminTasksCtrl])
         .controller('AdminOutdatedQueriesCtrl', ['$scope', 'Events', '$http', '$timeout', '$filter', AdminOutdatedQueriesCtrl])
})();
