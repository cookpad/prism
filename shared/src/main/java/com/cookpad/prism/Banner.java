package com.cookpad.prism;

public class Banner {
    final private static String banner =
        "    //   ) )\n" +
        "   //___/ /  __     ( )  ___      _   __\n" +
        "  / ____ / //  ) ) / / ((   ) ) // ) )  ) )\n" +
        " //       //      / /   \\ \\    // / /  / /\n" +
        "//       //      / / //   ) ) // / /  / /\n";
    public static String getBanner() {
        return Banner.banner;
    }
    private Banner() {}
}
