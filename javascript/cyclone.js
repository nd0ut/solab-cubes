String.prototype.supplant = function (o) {
    return this.replace(/{([^{}]*)}/g,
        function (a, b) {
            var r = o[b];
            return typeof r === 'string' || typeof r === 'number' ? r : a;
        }
    );
};

Array.prototype.clean = function(deleteValue) {
  for (var i = 0; i < this.length; i++) {
    if (this[i] == deleteValue) {
      this.splice(i, 1);
      i--;
    }
  }
  return this;
};

window.cyclon_ranges = function(lat, lon, radius) {
  var BEARING_HORIZONTAL = 90;
  var BEARING_VERTICAL = 0;

  var center = new LatLon(lat, lon);

  var max_lat = center.destinationPoint(BEARING_VERTICAL, radius).lat().toFixed(3);
  var min_lat = center.destinationPoint(BEARING_VERTICAL, -radius).lat().toFixed(3);

  var min_lon = center.destinationPoint(BEARING_HORIZONTAL, -radius).lon().toFixed(3);
  var max_lon = center.destinationPoint(BEARING_HORIZONTAL, radius).lon().toFixed(3);

  return {
    lat: {
      min: min_lat,
      max: max_lat
    },
    lon: {
      min: min_lon,
      max: max_lon
    }
  };
};

window.build_query = function(options) {
  var cyclon_ranges = window.cyclon_ranges(options.lat, options.lon, options.radius);

  return "http://geo.solab.rshu.ru:5000/cube/{cube_name}/aggregate?drilldown={drilldown}&cut=lat:{lat_min}--{lat_max}|lon:{lon_min}--{lon_max}|date:{date}".supplant({
    "cube_name": "wind",
    "drilldown": "speed|date:minute",
    "lat_min": cyclon_ranges.lat.min,
    "lat_max": cyclon_ranges.lat.max,
    "lon_min": cyclon_ranges.lon.min,
    "lon_max": cyclon_ranges.lon.max,
    "date": [options.date.year, options.date.month, options.date.day, options.date.hour, options.date.minute].clean().toString()
  });
};