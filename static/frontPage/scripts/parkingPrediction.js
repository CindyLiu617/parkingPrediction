/**
 *
 * Created by PPlovboiledfish on 2/28/16.
 */

var myApp = angular.module('uploadPage', []);

myApp.config(function ($interpolateProvider) {
    $interpolateProvider.startSymbol('//').endSymbol('//');
});

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
        this.uploadFileToUrl = function (file, uploadUrl, scope) {
            var fd = new FormData();
            fd.append('file', file);
            $http.post(uploadUrl, fd, {
                    transformRequest: angular.identity,
                    headers: {'Content-type': undefined}
                })
                .success(function (response) {
                    scope.receivedData = response.occupancyData;
                    var metaData = response.metaData;
                    scope.yearOption = metaData.yearOption;
                    scope.monthOption = metaData.monthOption;
                    scope.dateOption = metaData.dateOption;
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

myApp.directive('linearChart', ['$window', '$parse',
    function ($window, $parse) {
        return {
            restrict: "EA",
            template: "<svg width='850' height='200' display: block></svg>",
            transclude: true,
            scope: {
                chartData: '=chartData'
            },
            link: function (scope, elem, attrs) {

                function drawLineChart(dataToPlot, padding, pathClass, d3, svg, rawSvg) {
                    var xScale = d3.scale.linear()
                        .domain([dataToPlot[0].time.value, dataToPlot[dataToPlot.length - 1].time.value])
                        .range([padding + 5, rawSvg.clientWidth - padding]);

                    var yScale = d3.scale.linear()
                        .domain([d3.min(dataToPlot, function (d) {
                            return d.occupancy;
                        }), d3.max(dataToPlot, function (d) {
                            return d.occupancy;
                        })])
                        .range([rawSvg.clientHeight - padding, 0]);

                    var xAxisGen = d3.svg.axis()
                        .scale(xScale)
                        .orient("bottom")
                        .ticks(dataToPlot.length - 1);

                    var yAxisGen = d3.svg.axis()
                        .scale(yScale)
                        .orient("left")
                        .ticks(5);

                    var lineFun = d3.svg.line()
                        .x(function (d) {
                            return xScale(d.time.value);
                        })
                        .y(function (d) {
                            return yScale(d.occupancy);
                        })
                        .interpolate("basis");

                    svg.append("svg:g")
                        .attr("class", "x axis")
                        .attr("transform", "translate(0,180)")
                        .call(xAxisGen);

                    svg.append("svg:g")
                        .attr("class", "y axis")
                        .attr("transform", "translate(20,0)")
                        .call(yAxisGen);

                    svg.append("svg:path")
                        .attr({
                            d: lineFun(dataToPlot),
                            "stroke": "blue",
                            "stroke-width": 2,
                            "fill": "none",
                            "class": pathClass
                        });
                }

                scope.$watch('chartData', function (data) {
                    var padding = 20;

                    var pathClass = "path";
                    var d3 = $window.d3;
                    var rawSvg = elem.find("svg")[0];
                    var svg = d3.select(rawSvg);
                    if (data) {
                        debugger
                        d3.selectAll("svg > *").remove();
                        drawLineChart(data, padding, pathClass, d3, svg, rawSvg);
                    }
                });
            }
        };
    }]);

myApp.directive('ngsButterbar', ['$rootScope',
    function ($rootScope) {
        return {
            link: function (scope, element) {

                element.hide();

                $rootScope.$on('$routeChangeStart', function () {
                    element.show();
                    $('div[ng-view]').css('opacity', '0.5');
                });

                $rootScope.$on('$routeChangeSuccess', function () {
                    element.hide();
                    $('div[ng-view]').css('opacity', '1');
                });
            }
        };
    }]
);


myApp.controller('frontPageController', ['$scope', 'fileUpload',
    function ($scope, fileUpload) {
        $scope.uploadFile = function () {
            var file = $scope.myfiles;

            var uploadUrl = "/fileUpload";
            fileUpload.uploadFileToUrl(file, uploadUrl, $scope);
        };

        $scope.$watch('selectedYear', function (year) {
            if (year) {
                $scope.chartData = $scope.receivedData[year];
            }
        });
        $scope.$watch('selectedMonth', function (month) {
            if (month) {
                $scope.chartData = $scope.receivedData[month];
            }
        });
        $scope.$watch('selectedDay', function (day) {
            if (day) {
                $scope.chartData = $scope.receivedData[day];
            }
        });
    }]);
