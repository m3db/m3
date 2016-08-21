// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

'use strict';

var fs = require('fs');

module.exports = LicenseFixer;

function LicenseFixer(options) {
    options = options || {};
    this.slashLicense = null;
    this.hashLicense = null;
    this.licenseExpressions = [];
    this.dry = options.dry || false;
    this.silent = options.silent || false;
    this.verbose = options.verbose || false;
    Object.seal(this);
}

LicenseFixer.prototype.addLicense = function addLicense(license) {
    this.licenseExpressions.push(createLicenseExpression(license));
};

function createLicenseExpression(license) {
    license = license.trim();
    license = trimToCopyright(license);
    // Transform the license into a regular expression that matches the exact
    // license as well as similar licenses, with different dates and line
    // wraps.
    var pattern = license.split(/\s+/).map(relaxLicenseTerm).join('') + '\\s*';
    return new RegExp(pattern, 'gmi');
}

function trimToCopyright(license) {
    // Trim up to Copyright line (skipping prologues like "The MIT License (MIT)")
    var index = license.indexOf('Copyright');
    if (index >= 0) {
        license = license.slice(index);
    }
    return license;
}

function relaxLicenseTerm(term) {
    // There has been at least one occasion where someone replaced all single
    // quotes with double quotes throughout a file and got an extra license.
    return '\\s*((//|#)\\s*)*' + // wrap around any comment or spacing
        regexpEscape(term)
            .replace(/\d{4}/g, '\\d{4}') // dates to date patterns
            .replace(/['"]/g, '[\'"]'); // relax quotes
}

function regexpEscape(string) {
    return string.replace(regexpEscapePattern, '\\$&');
}

var regexpEscapePattern = /[|\\{}()[\]^$+*?.]/g;

LicenseFixer.prototype.setLicense = function setLicense(license) {
    license = trimToCopyright(license);
    this.slashLicense = createSlashLicense(license);
    this.hashLicense = createHashLicense(license);
};

function createSlashLicense(license) {
    return license.trim().split('\n').map(slashPrefix).join('');
}

function createHashLicense(license) {
    return license.trim().split('\n').map(hashPrefix).join('');
}

function slashPrefix(line) {
    return ('// ' + line).trim() + '\n';
}

function hashPrefix(line) {
    return ('# ' + line).trim() + '\n';
}

LicenseFixer.prototype.getLicenseForFile = function getLicenseForFile(file) {
    if (file.match(/\.(js|go|java)$/)) {
        return this.slashLicense;
    } else if (file.match(/\.(pyx?|pxd)$/)) {
        return this.hashLicense;
    }
    return null;
};

LicenseFixer.prototype.fixContent = function fixContent(file, content) {
    // separate the preamble shebang or such
    // TODO distinguish # encoding lines from license lines
    var preamble = '';
    if (content.match(/^#!|#\s*(en)?coding=/m)) {
        var index = content.indexOf('\n');
        if (index >= 0) {
            preamble = content.slice(0, index + 1);
            content = content.slice(index + 1).trim() + '\n';
        }
    }

    // Remove old licenses
    for (var i = 0; i < this.licenseExpressions.length; i++) {
        // string replace hangs in some pathelogical cases of repeated licenses
        // content = content.replace(this.licenseExpressions[i], '');
        var match;
        while (match = this.licenseExpressions[i].exec(content)) {
            content = content.slice(0, match.index) + content.slice(match.index + match[0].length);
        }
    }

    var license = this.getLicenseForFile(file);
    if (license == null) {
        if (!this.silent) {
            console.error('unrecognized file type', file);
        }
        return null;
    }

    // Reintroduce the preamble and license
    content = preamble + license + '\n' + content;

    return content;
};

LicenseFixer.prototype.fixFile = function fixFile(file) {
    var original = fs.readFileSync(file, 'utf8');

    if (original.length === 0) {
        // Ignore empty files
        if (this.verbose) {
            console.log('empty', file);
        }
        return false;
    }

    var content = this.fixContent(file, original);

    if (content == null) {
        // Return true on error so dry run fails
        return true;
    }

    if (original === content) {
        // No change
        if (this.verbose) {
            console.log('skip', file);
        }
        return false;
    }

    if (!this.silent) {
        console.log('fix', file);
    }

    if (this.dry) {
        return true;
    }

    fs.writeFileSync(file, content, 'utf8');
    return true;
};
