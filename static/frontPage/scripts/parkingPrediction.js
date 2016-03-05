/**
 *
 * Created by PPlovboiledfish on 2/28/16.
 */

var myApp = angular.module('uploadPage', []);

myApp.factory('alertsManager', function () {
    return {
        alerts: {},
        addAlert: function (message, type) {
            this.alerts[type] = this.alerts[type] || [];
            this.alerts[type].push(message);
        }
    };
});

myApp.service('fileUpload', ['$http', 'alertsManager',
    function ($http, alertsManager) {
    this.uploadFileToUrl = function (file, uploadUrl) {
        var fd = new FormData();
        fd.append('file', file);
        $http.post(uploadUrl, fd, {
                transformRequest: angular.identity,
                headers: {'Content-type': undefined}
            })
            .success(function (data) {
                debugger
            })
            .error(function () {
                alertsManager.addAlert('File Upload Fail! Please Try Again!', 'alert-error');
            });
    }
}]);

myApp.directive('fileModel', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        link: function (scope, element, attrs) {
            var model = $parse(attrs.fileModel);
            var modelSetter = model.assign;

            element.bind('change', function () {
                scope.$apply(function () {
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}]);

myApp.controller('frontPageController', ['$scope', 'fileUpload',
    function ($scope, fileUpload) {
    $scope.uploadFile = function () {
        var file = $scope.myfiles;

        var uploadUrl = "/fileUpload";
        fileUpload.uploadFileToUrl(file, uploadUrl);
    };

}]);

