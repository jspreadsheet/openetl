const path = require('path');

class MyPlugin {
    apply(compiler) {
        compiler.hooks.emit.tap('MyPlugin', (compilation) => {
            // Target the specific output file
            const fileName = 'index.js'; // Match output.filename

            // Ensure the file exists in assets
            if (!compilation.assets[fileName]) {
                console.error('Output file not found in compilation assets');
                return;
            }

            // Get the bundled file content
            const fileContent = compilation.assets[fileName].source();

            // Custom UMD header with axios passed to factory
            const header = `
if (! axios && typeof(require) === 'function') {
    var axios = require('axios');
}

;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    global.xero = factory();
}(this, (function () {`;

            const footer = `    return xero;
})));`;

            // Combine header, original content, and footer
            const updatedFileContent = header + '\n\n' + fileContent + '\n\n' + footer;

            // Replace the bundled file content
            compilation.assets[fileName] = {
                source: () => updatedFileContent,
                size: () => updatedFileContent.length,
            };
        });
    }
}

let dependencies = {
    axios: "axios"
}

const webpack = {
    target: [ 'node' ],
    entry: './src/index.ts',
    mode: 'production',
    resolve: {
        extensions: ['.ts', '.js'], // Resolve .ts and .js files
    },
    output: {
        filename: 'index.js',
        path: path.resolve(__dirname, 'dist'),
        library: 'xero',
        globalObject: 'default'
    },
    module: {
        rules: [
            {
                test: /\.ts$/, // Match .ts files
                use: 'ts-loader', // Use ts-loader to compile TypeScript
                exclude: /node_modules/,
            },
        ],
    },
    externals: dependencies,
    plugins: [new MyPlugin()],
    stats: {
        warnings: false
    },
    optimization: {
        minimize: false,
    },
};

module.exports = webpack;